package windowing

// Merge implementations. Each folds another accumulator of the SAME concrete
// type into the receiver. The session-window path merges overlapping windows by
// combining the earlier window's accumulators (base) with later ones, so
// first/last follow window start order.

func (a *sumAcc) Merge(other Accumulator) {
	o := other.(*sumAcc)
	a.sum += o.sum
	a.seen = a.seen || o.seen
}

func (a *avgAcc) Merge(other Accumulator) {
	o := other.(*avgAcc)
	a.sum += o.sum
	a.n += o.n
}

func (a *minAcc) Merge(other Accumulator) {
	o := other.(*minAcc)
	if o.seen && (!a.seen || o.min < a.min) {
		a.min = o.min
	}
	a.seen = a.seen || o.seen
}

func (a *maxAcc) Merge(other Accumulator) {
	o := other.(*maxAcc)
	if o.seen && (!a.seen || o.max > a.max) {
		a.max = o.max
	}
	a.seen = a.seen || o.seen
}

func (a *countAcc) Merge(other Accumulator) {
	a.n += other.(*countAcc).n
}

func (a *firstAcc) Merge(other Accumulator) {
	o := other.(*firstAcc)
	if !a.set && o.set {
		a.val = o.val
		a.set = true
	}
}

func (a *lastAcc) Merge(other Accumulator) {
	o := other.(*lastAcc)
	if o.set {
		a.val = o.val
		a.set = true
	}
}
