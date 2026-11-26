/*
 * Copyright 2026 The HAMi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package cleanup

import "time"

type confirmationState struct {
	firstSeen      time.Time
	candidateSince time.Time
	count          int
}

// Pod ownership is card-wide. On mixed nodes each shared logical die
// accumulates its own hardware observations within that card.
type confirmationKey struct {
	cardID  int32
	logicID int32
}

type confirmationTracker struct {
	states map[confirmationKey]confirmationState
}

func newConfirmationTracker() *confirmationTracker {
	return &confirmationTracker{states: make(map[confirmationKey]confirmationState)}
}

func (t *confirmationTracker) reset(key confirmationKey) bool {
	if _, exists := t.states[key]; !exists {
		return false
	}
	delete(t.states, key)
	return true
}

func (t *confirmationTracker) resetAll() int {
	count := len(t.states)
	t.states = make(map[confirmationKey]confirmationState)
	return count
}

func (t *confirmationTracker) observe(key confirmationKey, now, candidateSince time.Time, grace time.Duration, confirmations int) bool {
	state, exists := t.states[key]
	if !exists {
		state = confirmationState{firstSeen: now}
	}
	if !candidateSince.IsZero() && (state.candidateSince.IsZero() || candidateSince.After(state.candidateSince)) {
		state.candidateSince = candidateSince
	}
	state.count++
	t.states[key] = state
	return t.ready(key, now, grace, confirmations)
}

// recheckClaim updates an existing observation without counting another
// hardware scan. It returns true if Pod protection reset the observation.
func (t *confirmationTracker) recheckClaim(key confirmationKey, claim CardClaim) bool {
	if claim.Protected() {
		return t.reset(key)
	}
	state, exists := t.states[key]
	if exists && claim.CandidateSince.After(state.candidateSince) {
		state.candidateSince = claim.CandidateSince
		t.states[key] = state
	}
	return false
}

func (t *confirmationTracker) ready(key confirmationKey, now time.Time, grace time.Duration, confirmations int) bool {
	state, exists := t.states[key]
	if !exists {
		return false
	}
	if confirmations < 1 {
		confirmations = 1
	}
	// Never let a stale candidateSince (for example, restored from an older
	// snapshot after restart) shorten the current process's grace period.
	// firstSeen is the lower safety bound and is initialized only once.
	graceSince := state.firstSeen
	if state.candidateSince.After(graceSince) {
		graceSince = state.candidateSince
	}
	return state.count >= confirmations && now.Sub(graceSince) >= grace
}
