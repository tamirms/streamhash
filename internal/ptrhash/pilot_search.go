package ptrhash

import "unsafe"

// slotGenIsTaken checks slot occupancy using a raw pointer to the slotGen array.
// The caller must guarantee slot < len(s.slotGen). This is always true because
// slot comes from FastRange(hash, numSlots) where numSlots == len(slotGen).
// Using unsafe eliminates the bounds check that the compiler cannot elide when
// the slice header is hoisted into a local variable.
func slotGenIsTaken(slotGenBase unsafe.Pointer, gen uint8, slot uint32) bool {
	return *(*uint8)(unsafe.Add(slotGenBase, uintptr(slot))) == gen
}

// findFreePilot tries to find a pilot where all slots are free.
// Returns (pilot, true) if found, (0, false) if no free pilot exists.
// On success, writes computed slots to s.slotsBuffer for use by placeBucket.
func (s *solver) findFreePilot(bucket []bucketEntry, bucketSize int) (uint8, bool) {
	// Dispatch to size-specific implementations for hot path optimization.
	switch bucketSize {
	case 1:
		return s.findFreePilot1(bucket)
	case 2:
		return s.findFreePilot2(bucket)
	case 3:
		return s.findFreePilot3(bucket)
	case 4:
		return s.findFreePilot4(bucket)
	case 5:
		return s.findFreePilot5(bucket)
	case 6:
		return s.findFreePilot6(bucket)
	case 7:
		return s.findFreePilot7(bucket)
	case 8:
		return s.findFreePilot8(bucket)
	default:
		return s.findFreePilotSlice(bucket)
	}
}

// findFreePilot1 handles bucket size 1 (trivial case).
// On success, writes slot to s.slotsBuffer[0].
func (s *solver) findFreePilot1(bucket []bucketEntry) (uint8, bool) {
	// Hoist numSlots, slotGen base pointer, and generation into locals so the
	// compiler keeps them in registers across the 256-iteration pilot loop.
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0 := bucket[0]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		slot := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		if !slotGenIsTaken(slotGenBase, gen, slot) {
			s.slotsBuffer[0] = uint16(slot)
			return pilot, true
		}
	}
	return 0, false
}

// findFreePilot2 handles bucket size 2.
func (s *solver) findFreePilot2(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1 := bucket[0], bucket[1]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		slot0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		if slotGenIsTaken(slotGenBase, gen, slot0) {
			continue
		}
		slot1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		if slot0 == slot1 {
			// Internal collision - try next pilot
			continue
		}
		if slotGenIsTaken(slotGenBase, gen, slot1) {
			continue
		}
		s.slotsBuffer[0] = uint16(slot0)
		s.slotsBuffer[1] = uint16(slot1)
		return pilot, true
	}
	return 0, false
}

// findFreePilot3 handles bucket size 3.
func (s *solver) findFreePilot3(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2 := bucket[0], bucket[1], bucket[2]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		slot0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		slot1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		slot2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)

		if slotGenIsTaken(slotGenBase, gen, slot0) || slotGenIsTaken(slotGenBase, gen, slot1) || slotGenIsTaken(slotGenBase, gen, slot2) {
			continue
		}

		// Check internal collisions
		if slot0 == slot1 || slot0 == slot2 || slot1 == slot2 {
			continue
		}

		s.slotsBuffer[0] = uint16(slot0)
		s.slotsBuffer[1] = uint16(slot1)
		s.slotsBuffer[2] = uint16(slot2)
		return pilot, true
	}
	return 0, false
}

// findFreePilot4 handles bucket size 4.
func (s *solver) findFreePilot4(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2, e3 := bucket[0], bucket[1], bucket[2], bucket[3]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)
	hf3 := foldSlotInput(e3.k0, e3.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		slot0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		slot1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		slot2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)
		slot3 := pilotSlotFolded(hf3, s.pilotHPs[pilot], numSlots)

		if slotGenIsTaken(slotGenBase, gen, slot0) || slotGenIsTaken(slotGenBase, gen, slot1) || slotGenIsTaken(slotGenBase, gen, slot2) || slotGenIsTaken(slotGenBase, gen, slot3) {
			continue
		}

		// Check for internal collisions
		hasCollision := slot0 == slot1 || slot0 == slot2 || slot0 == slot3 ||
			slot1 == slot2 || slot1 == slot3 || slot2 == slot3
		if hasCollision {
			continue
		}

		s.slotsBuffer[0] = uint16(slot0)
		s.slotsBuffer[1] = uint16(slot1)
		s.slotsBuffer[2] = uint16(slot2)
		s.slotsBuffer[3] = uint16(slot3)
		return pilot, true
	}
	return 0, false
}

// findFreePilot5 handles bucket size 5.
func (s *solver) findFreePilot5(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2, e3, e4 := bucket[0], bucket[1], bucket[2], bucket[3], bucket[4]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)
	hf3 := foldSlotInput(e3.k0, e3.suffix)
	hf4 := foldSlotInput(e4.k0, e4.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		s0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		s1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		s2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)
		s3 := pilotSlotFolded(hf3, s.pilotHPs[pilot], numSlots)
		s4 := pilotSlotFolded(hf4, s.pilotHPs[pilot], numSlots)

		t0 := slotGenIsTaken(slotGenBase, gen, s0)
		t1 := slotGenIsTaken(slotGenBase, gen, s1)
		t2 := slotGenIsTaken(slotGenBase, gen, s2)
		t3 := slotGenIsTaken(slotGenBase, gen, s3)
		t4 := slotGenIsTaken(slotGenBase, gen, s4)
		if t0 || t1 || t2 || t3 || t4 {
			continue
		}

		noDup := s0 != s1 && s0 != s2 && s0 != s3 && s0 != s4 &&
			s1 != s2 && s1 != s3 && s1 != s4 &&
			s2 != s3 && s2 != s4 &&
			s3 != s4
		if noDup {
			s.slotsBuffer[0] = uint16(s0)
			s.slotsBuffer[1] = uint16(s1)
			s.slotsBuffer[2] = uint16(s2)
			s.slotsBuffer[3] = uint16(s3)
			s.slotsBuffer[4] = uint16(s4)
			return pilot, true
		}
	}
	return 0, false
}

// findFreePilot6 handles bucket size 6.
func (s *solver) findFreePilot6(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2, e3, e4, e5 := bucket[0], bucket[1], bucket[2], bucket[3], bucket[4], bucket[5]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)
	hf3 := foldSlotInput(e3.k0, e3.suffix)
	hf4 := foldSlotInput(e4.k0, e4.suffix)
	hf5 := foldSlotInput(e5.k0, e5.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		s0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		s1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		s2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)
		s3 := pilotSlotFolded(hf3, s.pilotHPs[pilot], numSlots)
		s4 := pilotSlotFolded(hf4, s.pilotHPs[pilot], numSlots)
		s5 := pilotSlotFolded(hf5, s.pilotHPs[pilot], numSlots)

		t0 := slotGenIsTaken(slotGenBase, gen, s0)
		t1 := slotGenIsTaken(slotGenBase, gen, s1)
		t2 := slotGenIsTaken(slotGenBase, gen, s2)
		t3 := slotGenIsTaken(slotGenBase, gen, s3)
		t4 := slotGenIsTaken(slotGenBase, gen, s4)
		t5 := slotGenIsTaken(slotGenBase, gen, s5)
		if t0 || t1 || t2 || t3 || t4 || t5 {
			continue
		}

		noDup := s0 != s1 && s0 != s2 && s0 != s3 && s0 != s4 && s0 != s5 &&
			s1 != s2 && s1 != s3 && s1 != s4 && s1 != s5 &&
			s2 != s3 && s2 != s4 && s2 != s5 &&
			s3 != s4 && s3 != s5 &&
			s4 != s5
		if noDup {
			s.slotsBuffer[0] = uint16(s0)
			s.slotsBuffer[1] = uint16(s1)
			s.slotsBuffer[2] = uint16(s2)
			s.slotsBuffer[3] = uint16(s3)
			s.slotsBuffer[4] = uint16(s4)
			s.slotsBuffer[5] = uint16(s5)
			return pilot, true
		}
	}
	return 0, false
}

// findFreePilot7 handles bucket size 7.
func (s *solver) findFreePilot7(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2, e3, e4, e5, e6 := bucket[0], bucket[1], bucket[2], bucket[3], bucket[4], bucket[5], bucket[6]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)
	hf3 := foldSlotInput(e3.k0, e3.suffix)
	hf4 := foldSlotInput(e4.k0, e4.suffix)
	hf5 := foldSlotInput(e5.k0, e5.suffix)
	hf6 := foldSlotInput(e6.k0, e6.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		s0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		s1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		s2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)
		s3 := pilotSlotFolded(hf3, s.pilotHPs[pilot], numSlots)
		s4 := pilotSlotFolded(hf4, s.pilotHPs[pilot], numSlots)
		s5 := pilotSlotFolded(hf5, s.pilotHPs[pilot], numSlots)
		s6 := pilotSlotFolded(hf6, s.pilotHPs[pilot], numSlots)

		t0 := slotGenIsTaken(slotGenBase, gen, s0)
		t1 := slotGenIsTaken(slotGenBase, gen, s1)
		t2 := slotGenIsTaken(slotGenBase, gen, s2)
		t3 := slotGenIsTaken(slotGenBase, gen, s3)
		t4 := slotGenIsTaken(slotGenBase, gen, s4)
		t5 := slotGenIsTaken(slotGenBase, gen, s5)
		t6 := slotGenIsTaken(slotGenBase, gen, s6)
		if t0 || t1 || t2 || t3 || t4 || t5 || t6 {
			continue
		}

		noDup := s0 != s1 && s0 != s2 && s0 != s3 && s0 != s4 && s0 != s5 && s0 != s6 &&
			s1 != s2 && s1 != s3 && s1 != s4 && s1 != s5 && s1 != s6 &&
			s2 != s3 && s2 != s4 && s2 != s5 && s2 != s6 &&
			s3 != s4 && s3 != s5 && s3 != s6 &&
			s4 != s5 && s4 != s6 &&
			s5 != s6
		if noDup {
			s.slotsBuffer[0] = uint16(s0)
			s.slotsBuffer[1] = uint16(s1)
			s.slotsBuffer[2] = uint16(s2)
			s.slotsBuffer[3] = uint16(s3)
			s.slotsBuffer[4] = uint16(s4)
			s.slotsBuffer[5] = uint16(s5)
			s.slotsBuffer[6] = uint16(s6)
			return pilot, true
		}
	}
	return 0, false
}

// findFreePilot8 handles bucket size 8.
func (s *solver) findFreePilot8(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	e0, e1, e2, e3 := bucket[0], bucket[1], bucket[2], bucket[3]
	e4, e5, e6, e7 := bucket[4], bucket[5], bucket[6], bucket[7]
	hf0 := foldSlotInput(e0.k0, e0.suffix)
	hf1 := foldSlotInput(e1.k0, e1.suffix)
	hf2 := foldSlotInput(e2.k0, e2.suffix)
	hf3 := foldSlotInput(e3.k0, e3.suffix)
	hf4 := foldSlotInput(e4.k0, e4.suffix)
	hf5 := foldSlotInput(e5.k0, e5.suffix)
	hf6 := foldSlotInput(e6.k0, e6.suffix)
	hf7 := foldSlotInput(e7.k0, e7.suffix)

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		s0 := pilotSlotFolded(hf0, s.pilotHPs[pilot], numSlots)
		s1 := pilotSlotFolded(hf1, s.pilotHPs[pilot], numSlots)
		s2 := pilotSlotFolded(hf2, s.pilotHPs[pilot], numSlots)
		s3 := pilotSlotFolded(hf3, s.pilotHPs[pilot], numSlots)
		s4 := pilotSlotFolded(hf4, s.pilotHPs[pilot], numSlots)
		s5 := pilotSlotFolded(hf5, s.pilotHPs[pilot], numSlots)
		s6 := pilotSlotFolded(hf6, s.pilotHPs[pilot], numSlots)
		s7 := pilotSlotFolded(hf7, s.pilotHPs[pilot], numSlots)

		t0 := slotGenIsTaken(slotGenBase, gen, s0)
		t1 := slotGenIsTaken(slotGenBase, gen, s1)
		t2 := slotGenIsTaken(slotGenBase, gen, s2)
		t3 := slotGenIsTaken(slotGenBase, gen, s3)
		t4 := slotGenIsTaken(slotGenBase, gen, s4)
		t5 := slotGenIsTaken(slotGenBase, gen, s5)
		t6 := slotGenIsTaken(slotGenBase, gen, s6)
		t7 := slotGenIsTaken(slotGenBase, gen, s7)
		if t0 || t1 || t2 || t3 || t4 || t5 || t6 || t7 {
			continue
		}

		noDup := s0 != s1 && s0 != s2 && s0 != s3 && s0 != s4 && s0 != s5 && s0 != s6 && s0 != s7 &&
			s1 != s2 && s1 != s3 && s1 != s4 && s1 != s5 && s1 != s6 && s1 != s7 &&
			s2 != s3 && s2 != s4 && s2 != s5 && s2 != s6 && s2 != s7 &&
			s3 != s4 && s3 != s5 && s3 != s6 && s3 != s7 &&
			s4 != s5 && s4 != s6 && s4 != s7 &&
			s5 != s6 && s5 != s7 &&
			s6 != s7
		if noDup {
			s.slotsBuffer[0] = uint16(s0)
			s.slotsBuffer[1] = uint16(s1)
			s.slotsBuffer[2] = uint16(s2)
			s.slotsBuffer[3] = uint16(s3)
			s.slotsBuffer[4] = uint16(s4)
			s.slotsBuffer[5] = uint16(s5)
			s.slotsBuffer[6] = uint16(s6)
			s.slotsBuffer[7] = uint16(s7)
			return pilot, true
		}
	}
	return 0, false
}

// findFreePilotSlice handles bucket sizes > 8.
func (s *solver) findFreePilotSlice(bucket []bucketEntry) (uint8, bool) {
	numSlots := s.numSlots
	slotGenBase := unsafe.Pointer(&s.slotGen[0])
	gen := s.generation

	n := len(bucket)
	slots := s.slotsBuffer[:n]
	folded := s.foldedBuffer[:n]
	r := (n / 4) * 4 // round down to multiple of 4 for chunked processing below

	// Precompute folded hash values outside the pilot loop.
	for i := range n {
		folded[i] = foldSlotInput(bucket[i].k0, bucket[i].suffix)
	}

	for pilotInt := range numPilotValues {
		pilot := uint8(pilotInt)
		anyTaken := false

		// Process in chunks of 4: compute slots then check occupancy
		for i := 0; i < r; i += 4 {
			slots[i] = uint16(pilotSlotFolded(folded[i], s.pilotHPs[pilot], numSlots))
			slots[i+1] = uint16(pilotSlotFolded(folded[i+1], s.pilotHPs[pilot], numSlots))
			slots[i+2] = uint16(pilotSlotFolded(folded[i+2], s.pilotHPs[pilot], numSlots))
			slots[i+3] = uint16(pilotSlotFolded(folded[i+3], s.pilotHPs[pilot], numSlots))
			t0 := slotGenIsTaken(slotGenBase, gen, uint32(slots[i]))
			t1 := slotGenIsTaken(slotGenBase, gen, uint32(slots[i+1]))
			t2 := slotGenIsTaken(slotGenBase, gen, uint32(slots[i+2]))
			t3 := slotGenIsTaken(slotGenBase, gen, uint32(slots[i+3]))
			if t0 || t1 || t2 || t3 {
				anyTaken = true
				break
			}
		}
		if !anyTaken {
			// Process remaining slots (n % 4)
			for i := r; i < n; i++ {
				slots[i] = uint16(pilotSlotFolded(folded[i], s.pilotHPs[pilot], numSlots))
				if slotGenIsTaken(slotGenBase, gen, uint32(slots[i])) {
					anyTaken = true
					break
				}
			}
		}
		if anyTaken {
			continue
		}

		if s.hasNoDuplicateSlots(slots) {
			return pilot, true
		}
	}
	return 0, false
}

// hasDuplicateSlotInput checks if any two entries in the bucket have the same
// slot input (k0 ^ suffix). Such entries produce identical slots for all pilots,
// making them indistinguishable to the MPHF.
func hasDuplicateSlotInput(bucket []bucketEntry) bool {
	for i := range bucket {
		xi := bucket[i].k0 ^ bucket[i].suffix
		for j := i + 1; j < len(bucket); j++ {
			if xi == bucket[j].k0^bucket[j].suffix {
				return true
			}
		}
	}
	return false
}

// hasNoDuplicateSlots checks if all slots are unique using generation-based O(n) detection.
// Uses generation+1 as a temporary marker to avoid a separate data structure.
// On exit (success or failure), all modified entries are reset to 0 so the
// main generation-based tracking is not corrupted.
func (s *solver) hasNoDuplicateSlots(slots []uint16) bool {
	localGen := s.generation + 1
	n := len(slots)
	for i := range n {
		slot := slots[i]
		if s.slotGen[slot] == localGen {
			for j := 0; j < i; j++ {
				s.slotGen[slots[j]] = 0
			}
			return false
		}
		s.slotGen[slot] = localGen
	}
	for i := range n {
		s.slotGen[slots[i]] = 0
	}
	return true
}

// hasNoDuplicateSlotsPhase2 checks if pre-computed slots have no duplicates.
// Uses a separate phase2SlotGen array to avoid corrupting the main slotGen
// tracking. Increments phase2Gen each call — no cleanup needed except on
// the rare uint32 wrap-around, where we clear the array to restore the invariant.
func (s *solver) hasNoDuplicateSlotsPhase2(slots []uint16) bool {
	s.phase2Gen++
	if s.phase2Gen == 0 {
		// uint32 wrapped around. Clear the array so no stale entry
		// can match localGen, then skip 0 (the zero-value of cleared entries).
		clear(s.phase2SlotGen)
		s.phase2Gen = 1
	}
	localGen := s.phase2Gen

	for _, slot := range slots {
		if s.phase2SlotGen[slot] == localGen {
			return false // Duplicate found
		}
		s.phase2SlotGen[slot] = localGen
	}
	return true
}
