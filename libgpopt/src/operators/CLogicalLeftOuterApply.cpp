//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterApply.cpp
//
//	@doc:
//		Implementation of left outer apply operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalLeftOuterApply.h"

#include "gpos/base.h"

#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::CLogicalLeftOuterApply
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::CLogicalLeftOuterApply(CMemoryPool *mp)
	: CLogicalApply(mp)
{
	GPOS_ASSERT(nullptr != mp);

	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::CLogicalLeftOuterApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::CLogicalLeftOuterApply(CMemoryPool *mp,
											   CColRefArray *pdrgpcrInner,
											   EOperatorId eopidOriginSubq)
	: CLogicalApply(mp, pdrgpcrInner, eopidOriginSubq)
{
	GPOS_ASSERT(0 < pdrgpcrInner->Size());
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::~CLogicalLeftOuterApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::~CLogicalLeftOuterApply() = default;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftOuterApply::DeriveMaxCard(CMemoryPool *,  // mp
									  CExpressionHandle &exprhdl) const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/,
							 exprhdl.DeriveMaxCard(0));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalLeftOuterApply::PstatsDerive(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 IStatisticsArray *	 // stats_ctxt
) const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// A left outer apply preserves every outer (child 0) row: each outer row
	// yields exactly one output row, with the inner columns NULL-extended when
	// the correlated inner produces no match (LOJ semantics). Consistently
	// with DeriveMaxCard(), which likewise derives from the outer child, the
	// output cardinality is that of the outer child.
	//
	// The base CLogicalApply::PstatsDerive() returns a dummy sized at the
	// generic CStatistics::DefaultRelationRows (1000). That placeholder is only
	// meant to survive until a decorrelation xform rewrites the apply into a
	// join whose real stats take over (apply is deliberately EspLow). But when
	// the apply cannot be decorrelated -- e.g. an IN/EXISTS subquery trapped
	// inside an OR, which must stay a correlated NL join to compute a per-row
	// boolean -- that dummy 1000 is what actually reaches costing, decoupling
	// the estimate from the real outer cardinality and misleading the join
	// method and row estimates of every operator above it. Size the dummy at
	// the outer child's real row count instead, preserving the invariant
	// Card(LOA) == Card(outer).
	CDouble rows = CStatistics::DefaultRelationRows;
	IStatistics *outer_stats = exprhdl.Pstats(0);
	if (nullptr != outer_stats)
	{
		rows = outer_stats->Rows();
	}

	return PstatsDeriveDummy(mp, exprhdl, rows);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftOuterApply::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterApply2LeftOuterJoin);
	(void) xform_set->ExchangeSet(
		CXform::ExfLeftOuterApply2LeftOuterJoinNoCorrelations);

	return xform_set;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftOuterApply::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	if (nullptr == m_pdrgpcrInner)
	{
		// LATERAL-derived Apply: no inner scalar colref. Use the 1-arg ctor
		// (the 2-arg form asserts pdrgpcrInner is non-null+non-empty).
		return GPOS_NEW(mp) CLogicalLeftOuterApply(mp);
	}

	CColRefArray *pdrgpcrInner =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcrInner, colref_mapping, must_exist);

	return GPOS_NEW(mp)
		CLogicalLeftOuterApply(mp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF
