package model

// controlCase is the bounded case in which a broken variant is searched for its
// minimal counterexample.
func controlCase(v Variant) *Case {
	switch v {
	case FloorlessPrune:
		return caseI()
	case AgeGC, RetargetCompaction:
		return caseG()
	case D5NoIdentity:
		return caseC0()
	case SupersedeUnchecked:
		return caseCD()
	case TailWithoutObject:
		return caseCD()
	case BatchLeaderReceipt:
		return caseB()
	case RideUnresolved:
		return caseBR()
	case FollowerStaleRead:
		return caseF()
	case DeleteAsRelease:
		return caseDT()
	case ReceiptAcrossIncarnation:
		return caseDU()
	case RecreateKeepsData:
		return caseDR()
	}
	return caseP()
}

// ControlCase is exported for the qual command.
func ControlCase(v Variant) *Case { return controlCase(v) }
