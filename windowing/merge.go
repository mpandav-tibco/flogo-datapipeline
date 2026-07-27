package windowing

import "github.com/mpandav-tibco/flogo-datapipeline/record"

// Merge implementations. Each folds another accumulator of the SAME concrete
// type into the receiver. The session-window path merges overlapping windows by
// combining the earlier window's accumulators (base) with later ones, so
// first/last follow window start order.

func (a *sumAcc) decSum() record.Decimal {
	if a.isDec {
		return a.dsum
	}
	d, _ := record.ToDecimal(a.sum)
	return d
}

func (a *sumAcc) Merge(other Accumulator) {
	o := other.(*sumAcc)
	if !o.seen {
		return
	}
	if a.isDec || o.isDec {
		a.dsum = a.decSum().Add(o.decSum())
		a.isDec = true
	} else {
		a.sum += o.sum
	}
	a.seen = true
}

func (a *avgAcc) decSum() record.Decimal {
	if a.isDec {
		return a.dsum
	}
	d, _ := record.ToDecimal(a.sum)
	return d
}

func (a *avgAcc) Merge(other Accumulator) {
	o := other.(*avgAcc)
	if o.n == 0 {
		return
	}
	if a.isDec || o.isDec {
		a.dsum = a.decSum().Add(o.decSum())
		a.isDec = true
	} else {
		a.sum += o.sum
	}
	a.n += o.n
}

func (a *minAcc) decMin() record.Decimal {
	if a.isDec {
		return a.dmin
	}
	d, _ := record.ToDecimal(a.min)
	return d
}

func (a *minAcc) Merge(other Accumulator) {
	o := other.(*minAcc)
	if !o.seen {
		return
	}
	if a.isDec || o.isDec {
		om := o.decMin()
		switch {
		case !a.seen:
			a.dmin = om
		case a.decMin().Cmp(om) > 0:
			a.dmin = om
		default:
			a.dmin = a.decMin()
		}
		a.isDec = true
	} else if !a.seen || o.min < a.min {
		a.min = o.min
	}
	a.seen = true
}

func (a *maxAcc) decMax() record.Decimal {
	if a.isDec {
		return a.dmax
	}
	d, _ := record.ToDecimal(a.max)
	return d
}

func (a *maxAcc) Merge(other Accumulator) {
	o := other.(*maxAcc)
	if !o.seen {
		return
	}
	if a.isDec || o.isDec {
		om := o.decMax()
		switch {
		case !a.seen:
			a.dmax = om
		case a.decMax().Cmp(om) < 0:
			a.dmax = om
		default:
			a.dmax = a.decMax()
		}
		a.isDec = true
	} else if !a.seen || o.max > a.max {
		a.max = o.max
	}
	a.seen = true
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
