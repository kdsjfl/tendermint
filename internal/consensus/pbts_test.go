package consensus

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tendermint/tendermint/abci/example/kvstore"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	tmtimemocks "github.com/tendermint/tendermint/libs/time/mocks"
	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"
	"github.com/tendermint/tendermint/types"
)

// pbtsTestHarness constructs a Tendermint network that can be used for testing the
// implementation of the Proposer-Based timestamps algorithm.
// It runs a series of consensus heights and captures timing of votes and events.
type pbtsTestHarness struct {
	// configuration options set by the user of the test harness.
	pbtsTestConfiguration

	// The Tendermint consensus state machine being run during
	// a run of the pbtsTestHarness.
	observedState *State

	// A stub for signing votes and messages using the key
	// from the observedState.
	observedValidator *validatorStub

	// A list of simulated validators that interact with the observedState and are
	// fully controlled by the test harness.
	otherValidators []*validatorStub

	// The mock time source used by all of the validator stubs in the test harness.
	// This mock clock allows the test harness to produce votes and blocks with arbitrary
	// timestamps.
	validatorClock *tmtimemocks.Source

	chainID string

	// channels for verifying that the observed validator completes certain actions.
	ensureProposalCh, roundCh, blockCh, ensureVoteCh <-chan tmpubsub.Message

	currentHeight int64
	currentRound  int32

	t *testing.T
}

type pbtsTestConfiguration struct {
	// The timestamp consensus parameters to be used by the state machine under test.
	timestampParams types.TimestampParams

	// The setting to use for the TimeoutPropose configuration parameter.
	timeoutPropose time.Duration

	// The timestamp of the first block produced by the network.
	genesisTime time.Time

	// The time at which the proposal at height 2 should be delivered.
	height2ProposalDeliverTime time.Time

	// The timestamp of the block proposed at height 2.
	height2ProposedBlockTime time.Time
}

func newPBTSTestHarness(t *testing.T, tc pbtsTestConfiguration) pbtsTestHarness {
	const validators = 4
	cfg := configSetup(t)
	clock := new(tmtimemocks.Source)
	cfg.Consensus.TimeoutPropose = tc.timeoutPropose
	consensusParams := types.DefaultConsensusParams()
	consensusParams.Timestamp = tc.timestampParams

	state, privVals := makeGenesisState(cfg, genesisStateArgs{
		Params:     consensusParams,
		Time:       tc.genesisTime,
		Validators: validators,
	})
	cs := newState(state, privVals[0], kvstore.NewApplication())
	vss := make([]*validatorStub, validators)
	for i := 0; i < validators; i++ {
		vss[i] = newValidatorStub(privVals[i], int32(i))
	}
	incrementHeight(vss[1:]...)

	for _, vs := range vss {
		vs.clock = clock
	}
	pubKey, err := vss[0].PrivValidator.GetPubKey(context.Background())
	assert.NoError(t, err)

	return pbtsTestHarness{
		pbtsTestConfiguration: tc,
		observedValidator:     vss[0],
		observedState:         cs,
		otherValidators:       vss[1:],
		validatorClock:        clock,
		currentHeight:         1,
		chainID:               cfg.ChainID(),
		roundCh:               subscribe(cs.eventBus, types.EventQueryNewRound),
		ensureProposalCh:      subscribe(cs.eventBus, types.EventQueryCompleteProposal),
		blockCh:               subscribe(cs.eventBus, types.EventQueryNewBlock),
		ensureVoteCh:          subscribeToVoterBuffered(cs, pubKey.Address()),
		t:                     t,
	}
}

func (p *pbtsTestHarness) genesisHeight() {
	p.validatorClock.On("Now").Return(p.height2ProposedBlockTime).Times(8)

	startTestRound(p.observedState, p.currentHeight, p.currentRound)
	ensureNewRound(p.t, p.roundCh, p.currentHeight, p.currentRound)
	propBlock, partSet := p.observedState.createProposalBlock()
	bid := types.BlockID{Hash: propBlock.Hash(), PartSetHeader: partSet.Header()}
	ensureProposal(p.t, p.ensureProposalCh, p.currentHeight, p.currentRound, bid)
	ensurePrevote(p.t, p.ensureVoteCh, p.currentHeight, p.currentRound)
	signAddVotes(p.observedState, tmproto.PrevoteType, p.chainID, bid, p.otherValidators...)

	signAddVotes(p.observedState, tmproto.PrecommitType, p.chainID, bid, p.otherValidators...)
	ensurePrecommit(p.t, p.ensureVoteCh, p.currentHeight, p.currentRound)

	ensureNewBlock(p.t, p.blockCh, p.currentHeight)
	p.currentHeight++
	incrementHeight(p.otherValidators...)
}

func (p *pbtsTestHarness) height2() heightResult {
	signer := p.otherValidators[0].PrivValidator
	return p.nextHeight(signer, p.height2ProposalDeliverTime, p.height2ProposedBlockTime, time.Now())
}

func (p *pbtsTestHarness) nextHeight(proposer types.PrivValidator, deliverTime, proposedTime, nextProposedTime time.Time) heightResult {
	p.validatorClock.On("Now").Return(nextProposedTime).Times(8)
	pubKey, err := p.observedValidator.PrivValidator.GetPubKey(context.Background())
	assert.NoError(p.t, err)
	resultCh := collectResults(p.t, p.observedState.eventBus, pubKey.Address())

	ensureNewRound(p.t, p.roundCh, p.currentHeight, p.currentRound)

	b, _ := p.observedState.createProposalBlock()
	b.Height = p.currentHeight
	b.Header.Height = p.currentHeight
	b.Header.Time = proposedTime

	k, err := proposer.GetPubKey(context.Background())
	assert.NoError(p.t, err)
	b.Header.ProposerAddress = k.Address()
	ps := b.MakePartSet(types.BlockPartSizeBytes)
	bid := types.BlockID{Hash: b.Hash(), PartSetHeader: ps.Header()}
	prop := types.NewProposal(p.currentHeight, 0, -1, bid)
	tp := prop.ToProto()

	if err := proposer.SignProposal(context.Background(), p.observedState.state.ChainID, tp); err != nil {
		p.t.Fatalf("error signing proposal: %s", err)
	}

	time.Sleep(time.Until(deliverTime))
	prop.Signature = tp.Signature
	if err := p.observedState.SetProposalAndBlock(prop, b, ps, "peerID"); err != nil {
		p.t.Fatal(err)
	}
	ensureProposal(p.t, p.ensureProposalCh, p.currentHeight, 0, bid)

	ensurePrevote(p.t, p.ensureVoteCh, p.currentHeight, p.currentRound)
	signAddVotes(p.observedState, tmproto.PrevoteType, p.chainID, bid, p.otherValidators...)

	signAddVotes(p.observedState, tmproto.PrecommitType, p.chainID, bid, p.otherValidators...)
	ensurePrecommit(p.t, p.ensureVoteCh, p.currentHeight, p.currentRound)

	p.currentHeight++
	incrementHeight(p.otherValidators...)
	return <-resultCh
}

func collectResults(t *testing.T, eb *types.EventBus, address []byte) <-chan heightResult {
	t.Helper()
	resultCh := make(chan heightResult)
	voteSub, err := eb.SubscribeUnbuffered(context.Background(), "voteSubscriber", types.EventQueryVote)
	assert.NoError(t, err)
	go func() {
		var res heightResult
		for {
			voteMsg := <-voteSub.Out()
			ts := time.Now()
			vote := voteMsg.Data().(types.EventDataVote)
			if !bytes.Equal(address, vote.Vote.ValidatorAddress) {
				continue
			}
			voteEvent, _ := voteMsg.Data().(types.EventDataVote)
			if voteEvent.Vote.Type != tmproto.PrevoteType {
				continue
			}
			res.prevoteIssuedAt = ts
			res.prevote = voteEvent.Vote
			break
		}
		err := eb.UnsubscribeAll(context.Background(), "voteSubscriber")
		assert.NoError(t, err)
		resultCh <- res
		close(resultCh)
	}()
	return resultCh
}

func (p *pbtsTestHarness) run() resultSet {
	p.genesisHeight()
	r2 := p.height2()
	return resultSet{
		height2: r2,
	}
}

type resultSet struct {
	height2 heightResult
}

type heightResult struct {
	prevote         *types.Vote
	prevoteIssuedAt time.Time
}

// TestReceiveProposalWaitsForPreviousBlockTime tests that a validator receiving
// a proposal waits until the previous block time passes before issuing a prevote.
// The test delivers the block to the validator after the configured `timeout-propose`,
// but before the proposer-based timestamp bound on block delivery and checks that
// the consensus algorithm correctly waits for the new block to be delivered
// and issues a prevote for it.
func TestReceiveProposalWaitsForPreviousBlockTime(t *testing.T) {
	initialTime := time.Now().Add(50 * time.Millisecond)
	cfg := pbtsTestConfiguration{
		timestampParams: types.TimestampParams{
			Precision: 100 * time.Millisecond,
			MsgDelay:  500 * time.Millisecond,
		},
		timeoutPropose:             50 * time.Millisecond,
		genesisTime:                initialTime,
		height2ProposalDeliverTime: initialTime.Add(450 * time.Millisecond),
		height2ProposedBlockTime:   initialTime.Add(350 * time.Millisecond),
	}

	pbtsTest := newPBTSTestHarness(t, cfg)
	results := pbtsTest.run()

	// Check that the validator waited until after the proposer-based timestamp
	// waitinTime bound.
	assert.True(t, results.height2.prevoteIssuedAt.After(cfg.height2ProposalDeliverTime))
	maxWaitingTime := cfg.genesisTime.Add(cfg.timestampParams.Precision).Add(cfg.timestampParams.MsgDelay)
	assert.True(t, results.height2.prevoteIssuedAt.Before(maxWaitingTime))

	// Check that the validator did not prevote for nil.
	assert.NotNil(t, results.height2.prevote.BlockID.Hash)

}

// TestReceiveProposalTimesOutOnSlowDelivery tests that a validator receiving
// a proposal times out and prevotes nil if the block is not delivered by the
// within the proposer-based timestamp algorithm's waitingTime bound.
// The test delivers the block to the validator after the previous block's time
// and after the proposer-based timestamp bound on block delivery.
// The test then checks that the validator correctly waited for the new block
// and prevoted nil after timing out.
func TestReceiveProposalTimesOutOnSlowDelivery(t *testing.T) {
	initialTime := time.Now()
	cfg := pbtsTestConfiguration{
		timestampParams: types.TimestampParams{
			Precision: 100 * time.Millisecond,
			MsgDelay:  500 * time.Millisecond,
		},
		timeoutPropose:             50 * time.Millisecond,
		genesisTime:                initialTime,
		height2ProposalDeliverTime: initialTime.Add(610 * time.Millisecond),
		height2ProposedBlockTime:   initialTime.Add(350 * time.Millisecond),
	}

	pbtsTest := newPBTSTestHarness(t, cfg)
	results := pbtsTest.run()

	// Check that the validator waited until after the proposer-based timestamp
	// waitinTime bound.
	maxWaitingTime := initialTime.Add(cfg.timestampParams.Precision).Add(cfg.timestampParams.MsgDelay)
	assert.True(t, results.height2.prevoteIssuedAt.After(maxWaitingTime))

	// Ensure that the validator issued a prevote for nil.
	assert.Nil(t, results.height2.prevote.BlockID.Hash)
}

func TestProposerWaitTime(t *testing.T) {
	genesisTime, err := time.Parse(time.RFC3339, "2019-03-13T23:00:00Z")
	require.NoError(t, err)
	testCases := []struct {
		name              string
		previousBlockTime time.Time
		localTime         time.Time
		expectedWait      time.Duration
	}{
		{
			name:              "block time greater than local time",
			previousBlockTime: genesisTime.Add(5 * time.Nanosecond),
			localTime:         genesisTime.Add(1 * time.Nanosecond),
			expectedWait:      4 * time.Nanosecond,
		},
		{
			name:              "local time greater than block time",
			previousBlockTime: genesisTime.Add(1 * time.Nanosecond),
			localTime:         genesisTime.Add(5 * time.Nanosecond),
			expectedWait:      0,
		},
		{
			name:              "both times equal",
			previousBlockTime: genesisTime.Add(5 * time.Nanosecond),
			localTime:         genesisTime.Add(5 * time.Nanosecond),
			expectedWait:      0,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mockSource := new(tmtimemocks.Source)
			mockSource.On("Now").Return(testCase.localTime)

			ti := proposerWaitTime(mockSource, testCase.previousBlockTime)
			assert.Equal(t, testCase.expectedWait, ti)
		})
	}
}

func TestProposalTimeout(t *testing.T) {
	genesisTime, err := time.Parse(time.RFC3339, "2019-03-13T23:00:00Z")
	require.NoError(t, err)
	testCases := []struct {
		name              string
		localTime         time.Time
		previousBlockTime time.Time
		precision         time.Duration
		msgDelay          time.Duration
		expectedDuration  time.Duration
	}{
		{
			name:              "MsgDelay + Precision has not quite elapsed",
			localTime:         genesisTime.Add(525 * time.Millisecond),
			previousBlockTime: genesisTime.Add(6 * time.Millisecond),
			precision:         time.Millisecond * 20,
			msgDelay:          time.Millisecond * 500,
			expectedDuration:  1 * time.Millisecond,
		},
		{
			name:              "MsgDelay + Precision equals current time",
			localTime:         genesisTime.Add(525 * time.Millisecond),
			previousBlockTime: genesisTime.Add(5 * time.Millisecond),
			precision:         time.Millisecond * 20,
			msgDelay:          time.Millisecond * 500,
			expectedDuration:  0,
		},
		{
			name:              "MsgDelay + Precision has elapsed",
			localTime:         genesisTime.Add(725 * time.Millisecond),
			previousBlockTime: genesisTime.Add(5 * time.Millisecond),
			precision:         time.Millisecond * 20,
			msgDelay:          time.Millisecond * 500,
			expectedDuration:  0,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			mockSource := new(tmtimemocks.Source)
			mockSource.On("Now").Return(testCase.localTime)

			tp := types.TimestampParams{
				Precision: testCase.precision,
				MsgDelay:  testCase.msgDelay,
			}

			ti := proposalStepWaitingTime(mockSource, testCase.previousBlockTime, tp)
			assert.Equal(t, testCase.expectedDuration, ti)
		})
	}
}
