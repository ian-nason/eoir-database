# Data Dictionary

Source: [EOIR FOIA Library](https://www.justice.gov/eoir/foia-library-0)

## appeal2

Loaded from tblAppeal2.csv

Source file: `tblAppeal2.csv`
Rows: 1,283,643

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| idnProceeding | INTEGER | 9.8% | 130236 |  |
| idncase | INTEGER | 0.1% | 2068887 |  |
| idnAppeal | INTEGER | 0.0% | 1 |  |

## appeal_fed_courts

Loaded from tblAppealFedCourts.csv

Source file: `tblAppealFedCourts.csv`
Rows: 183,628

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strFedCourtDecision | VARCHAR | 96.6% | REM |  |
| datRequestedByOIL | DATE | 3.2% | 2011-11-10 |  |
| lngAppealID | VARCHAR | 5.4% | 4901311 |  |
| idnAppealFedCourts | INTEGER | 0.0% | 1491870 |  |

## appeals

Appeals to Board of Immigration Appeals

Source file: `tblAppeal.csv`
Rows: 1,585,540

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strProbono | VARCHAR | 99.8% | Y |  |
| strCustody | VARCHAR | 15.5% | R |  |
| strProceedingIHP | VARCHAR | 98.0% | S |  |
| strNat | VARCHAR | 8.5% | MX |  |
| strLang | VARCHAR | 8.7% | ENG |  |
| strCaseType | VARCHAR | 2.9% | RMV |  |
| strBIADecisionType | VARCHAR | 20.6% | R |  |
| strBIADecision | VARCHAR | 19.0% | MBD |  |
| datBIADecision | DATE | 19.0% | 2018-08-09 |  |
| datAttorneyE27 | DATE | 30.2% | 2018-03-16 |  |
| strFiledBy | VARCHAR | 0.0% | A |  |
| datAppealFiled | DATE | 0.0% | 2018-03-15 |  |
| strAppealType | VARCHAR | 0.0% | Bond Appeal |  |
| strAppealCategory | VARCHAR | 0.0% | IJ |  |
| idnProceeding | INTEGER | 8.5% | 7816197 |  |
| idncase | INTEGER | 0.1% | 8310833 |  |
| idnAppeal | INTEGER | 0.0% | 5173725 |  |

## applications

Applications for relief (asylum, cancellation, etc.)

Source file: `tbl_Court_Appln.csv`
Rows: 16,460,394

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| APPL_DEC | VARCHAR | 35.8% | D |  |
| APPL_RECD_DATE | DATE | 0.0% | 1989-08-31 |  |
| APPL_CODE | VARCHAR | 0.0% | ASYL | Joins to lu_application.APPL_CODE |
| IDNCASE | INTEGER | 0.0% | 2427262 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 597160 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNPROCEEDINGAPPLN | INTEGER | 0.0% | 112039 |  |

## attorneys

Attorney registry

Source file: `tbl_EOIR_Attorney.csv`
Rows: 411,877

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2024-03-19 |  |
| datCreatedOn | DATE | 0.0% | 2022-08-26 |  |
| Source_Flag | VARCHAR | 0.0% | eReg |  |
| blnAttorneyActive | BOOLEAN | 0.0% | false |  |
| BaseCityCode | VARCHAR | 33.9% | BIA |  |
| OldAttorneyID | VARCHAR | 37.3% | 8801 |  |
| EOIRAttorneyID | VARCHAR | 0.0% | D10A1B8A-732F-44AF-A801-0000371C7F67 |  |

## bonds

Bond hearing records and amounts

Source file: `D_TblAssociatedBond.csv`
Rows: 1,654,849

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DECISION_DUE_DATE | DATE | 99.7% | 2005-02-24 |  |
| FILING_PARTY | VARCHAR | 26.4% | BD |  |
| FILING_METHOD | VARCHAR | 28.5% | BD |  |
| BOND_TYPE | VARCHAR | 4.6% | BD |  |
| E_28_DATE | DATE | 89.8% | 2001-01-04 |  |
| DATE_APPEAL_DUE | DATE | 8.0% | 1999-03-25 |  |
| APPEAL_NOT_FILED | VARCHAR | 99.0% | X |  |
| APPEAL_REVD | VARCHAR | 75.8% | A |  |
| NEW_BOND | DOUBLE | 52.5% | 10000.0 |  |
| BOND_HEAR_REQ_DATE | DATE | 0.0% | 2000-12-20 |  |
| BOND_HEARING_TELEPHONIC | VARCHAR | 91.9% | P |  |
| INS_TA | VARCHAR | 79.3% | BAC |  |
| REL_CON | VARCHAR | 89.6% | N |  |
| INITIAL_BOND | DOUBLE | 83.5% | 4500.0 |  |
| COMP_DATE | DATE | 0.4% | 2001-01-26 |  |
| DEC | VARCHAR | 0.4% | C |  |
| IJ_CODE | VARCHAR | 8.5% | GMR | Joins to lu_judge.JUDGE_CODE |
| HEARING_LOC_CODE | VARCHAR | 0.0% | ATD | Joins to lu_hearing_location.LOC_CODE |
| BASE_CITY_NAME | VARCHAR | 54.1% | ATLANTA |  |
| BASE_CITY_CODE | VARCHAR | 0.0% | ATD | Joins to lu_base_city.BASE_CITY_CODE |
| INPUT_TIME | VARCHAR | 0.0% | 1140 |  |
| INPUT_DATE | DATE | 0.0% | 2001-01-04 |  |
| UPDATE_TIME | VARCHAR | 0.0% | 0002 |  |
| UPDATE_DATE | DATE | 0.0% | 2019-10-11 |  |
| SUB_GENERATION | VARCHAR | 0.0% | 99 |  |
| GENERATION | VARCHAR | 0.0% | 99 |  |
| REC_TYPE | VARCHAR | 0.0% | B |  |
| OSC_DATE | DATE | 7.0% | 1900-01-01 |  |
| IDNCASE | INTEGER | 0.0% | 2047244 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 5 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNASSOCBOND | INTEGER | 0.0% | 2 |  |

## case_identifiers

Case ID cross-references

Source file: `A_TblCaseIdentifier.csv`
Rows: 2,669,114

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| CASE_ID | VARCHAR | 0.0% | SR |  |
| IDNCASE | INTEGER | 0.0% | 5716149 | Primary case ID, joins across all core tables |
| IDNCASEID | INTEGER | 0.0% | 198607 |  |

## case_priority_history

Priority code changes

Source file: `tbl_CasePriorityHistory.csv`
Rows: 151,250

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATMODIFIEDON | DATE | 0.0% | 2014-08-06 |  |
| DATCREATEDON | DATE | 0.0% | 2014-08-06 |  |
| idnCase | INTEGER | 0.0% | 7482576 |  |
| casePriority_code | VARCHAR | 0.0% | N/A |  |
| idnCasePriHistory | INTEGER | 0.0% | 103 |  |

## cases

Case-level demographics, custody, dates, attorney info

Source file: `A_TblCase.csv`
Rows: 12,869,593

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| CASEPRIORITY_CODE | VARCHAR | 0.0% | N/A |  |
| DETENTION_FACILITY_TYPE | VARCHAR | 100.0% | M |  |
| DCO_LOCATION | VARCHAR | 100.0% | Eloy Arizona |  |
| DETENTION_LOCATION | VARCHAR | 100.0% | VIC- CORRECTIONAL INSTITUTION |  |
| DETENTION_DATE | DATE | 100.0% |  |  |
| LPR | BOOLEAN | 91.7% | false |  |
| DATE_RELEASED | DATE | 92.4% | 2005-05-11 |  |
| DATE_DETAINED | DATE | 81.1% | 2024-06-24 |  |
| Sex | VARCHAR | 44.9% | M |  |
| ZBOND_MRG_FLAG | VARCHAR | 100.0% | Y |  |
| ADDRESS_CHANGEDON | DATE | 61.9% | 2024-10-16 |  |
| UPDATED_STATE | VARCHAR | 99.9% | FL |  |
| C_RELEASE_DATE | DATE | 99.2% | 2009-08-14 |  |
| C_BIRTHDATE | DATE | 100.0% | 2024-05-31 |  |
| C_ASY_TYPE | VARCHAR | 59.9% | I |  |
| DATE_OF_ENTRY | DATE | 38.7% | 2019-08-18 |  |
| INMATE_HOUSING | VARCHAR | 98.1% | LIV |  |
| RELEASE_YEAR | VARCHAR | 79.1% | 0000 |  |
| RELEASE_MONTH | VARCHAR | 79.1% | 00 |  |
| CORRECTIONAL_FAC | VARCHAR | 97.8% | S |  |
| UP_BOND_RSN | VARCHAR | 100.0% | 09 |  |
| UP_BOND_DATE | DATE | 100.0% | 1999-09-09 |  |
| LATEST_CAL_TYPE | VARCHAR | 19.9% | M |  |
| LATEST_TIME | VARCHAR | 19.4% | 0830 |  |
| LATEST_HEARING | DATE | 19.4% | 2024-07-24 |  |
| UPDATE_SITE | VARCHAR | 1.0% | SDC |  |
| CASE_TYPE | VARCHAR | 0.0% | CFR | Joins to lu_case_type.CASE_TYPE_CODE |
| ATTY_NBR | VARCHAR | 39.0% | 0 |  |
| E_28_DATE | DATE | 58.4% | 2025-04-04 |  |
| SITE_TYPE | VARCHAR | 50.6% | M |  |
| CUSTODY | VARCHAR | 1.0% | D | Joins to lu_custody_status.CUSTODY_CODE |
| LANG | VARCHAR | 1.2% | SP | Joins to lu_language.LANG_CODE |
| NAT | VARCHAR | 1.0% | BL | Joins to lu_nationality.NAT_CODE |
| UPDATED_CITY | VARCHAR | 99.9% | MIAMI |  |
| UPDATED_ZIPCODE | VARCHAR | 99.9% | 33194 |  |
| ALIEN_ZIPCODE | VARCHAR | 1.5% | 31537 |  |
| ALIEN_STATE | VARCHAR | 3.0% | GA |  |
| ALIEN_CITY | VARCHAR | 3.0% | Folkston |  |
| IDNCASE | INTEGER | 0.0% | 14258892 | Primary case ID, joins across all core tables |

## charges

Individual charges per proceeding

Source file: `B_TblProceedCharges.csv`
Rows: 19,270,794

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| CHG_STATUS | VARCHAR | 16.1% | O |  |
| CHARGE | VARCHAR | 0.0% | 212a20 |  |
| IDNPROCEEDING | INTEGER | 0.0% | 1 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNCASE | INTEGER | 0.0% | 2047228 | Primary case ID, joins across all core tables |
| IDNPRCDCHG | INTEGER | 0.0% | 1 |  |

## custody_history

Custody status change records

Source file: `tbl_CustodyHistory.csv`
Rows: 10,279,713

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATRELEASED | DATE | 89.7% | 2003-11-06 |  |
| DATDETAINED | DATE | 77.6% | 2003-08-19 |  |
| CUSTODY | VARCHAR | 0.0% | N | Joins to lu_custody_status.CUSTODY_CODE |
| IDNCASE | INTEGER | 0.0% | 2433318 | Primary case ID, joins across all core tables |
| IDNCUSTODY | INTEGER | 0.0% | 29 |  |

## eoirdb_schema

Loaded from EOIRDB_Schema.csv

Source file: `EOIRDB_Schema.csv`
Rows: 1,051

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Constraints  (PK/NULL) | VARCHAR | 0.0% | NO |  |
| name | VARCHAR | 0.0% | int |  |
| field_name | VARCHAR | 0.0% | IDNCASE |  |
| table_name | VARCHAR | 0.0% | A_TblCase |  |

## juvenile_history

Juvenile designation records

Source file: `tbl_JuvenileHistory.csv`
Rows: 3,183,371

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATMODIFIEDON | DATE | 62.3% | 2014-09-08 |  |
| DATCREATEDON | DATE | 1.2% | 2014-09-06 |  |
| idnJuvenile | INTEGER | 0.0% | 1 |  |
| idnProceeding | INTEGER | 0.0% | 3200129 |  |
| idnCase | INTEGER | 0.0% | 2046990 |  |
| idnJuvenileHistory | INTEGER | 0.0% | 5 |  |

## lead_rider

Lead/rider case relationships

Source file: `tbl_Lead_Rider.csv`
Rows: 2,678,697

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| idnRiderProceedingEnd | INTEGER | 88.1% | 113050 |  |
| idnRiderProceedingStart | INTEGER | 0.0% | 3200072 |  |
| idnLeadProceedingEnd | INTEGER | 88.1% | 1169257 |  |
| idnLeadProceedingStart | INTEGER | 0.0% | 3200073 |  |
| datSeveredOn | DATE | 88.1% | 2011-02-03 |  |
| datModifiedOn | DATE | 0.0% | 2011-01-15 |  |
| datCreatedOn | DATE | 0.0% | 2011-01-15 |  |
| idnRiderCase | INTEGER | 0.0% | 2046938 |  |
| idnLeadCase | INTEGER | 0.0% | 2046939 |  |
| idnLeadRider | INTEGER | 0.0% | 1 |  |

## lu_3_mbr_referred_reason

Loaded from tblLookup3MbrReferredReason.csv

Source file: `tblLookup3MbrReferredReason.csv`
Rows: 7

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2002-09-13 |  |
| datCreatedOn | DATE | 0.0% | 2002-09-13 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strReason | VARCHAR | 0.0% | Inconsistency Among Judges |  |
| idnReason | INTEGER | 0.0% | 1 |  |

## lu_adjournment

Adjournment reason codes

Source file: `tblAdjournmentcodes.csv`
Rows: 124

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| asy_clk_stat | VARCHAR | 0.0% | S |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 28.2% | 2003-09-24 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| Regd_By | VARCHAR | 0.0% | A |  |
| strDesciption | VARCHAR | 0.0% | Respondent to seek representation |  |
| strcode | VARCHAR | 0.0% | 01 |  |
| idnAdjCode | INTEGER | 0.0% | 1 |  |

## lu_alien_nat

Loaded from tblLookupAlienNat.csv

Source file: `tblLookupAlienNat.csv`
Rows: 256

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 3.1% | 2019-10-05 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-02 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| Nacara | VARCHAR | 5.5% | N |  |
| strDescription | VARCHAR | 0.0% | <All> |  |
| strCode | VARCHAR | 0.0% | <A> |  |
| idnNat | INTEGER | 0.0% | 252 |  |

## lu_allegation_stmt

Loaded from tblLookUpAllegationStmt.csv

Source file: `tblLookUpAllegationStmt.csv`
Rows: 3

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strDescription | VARCHAR | 0.0% | You are an arriving alien |  |
| strcode | VARCHAR | 0.0% | A1 |  |
| idnAllegStmt | INTEGER | 0.0% | 1 |  |

## lu_app_decision

Application decision codes

Source file: `tblLookupCourtAppDecisions.csv`
Rows: 14

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Comments | VARCHAR | 85.7% | code will be use to adjudicate as "moot" when multiple applications are filed |  |
| datModifiedOn | DATE | 64.3% | 2003-12-18 |  |
| datCreatedOn | DATE | 0.0% | 2003-01-31 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strCourtApplnDecDesc | VARCHAR | 0.0% | ABANDONMENT |  |
| strCourtApplnDecCode | VARCHAR | 0.0% | A |  |
| idnCourtApplnDec | INTEGER | 0.0% | 5 |  |

## lu_appeal_issues

Loaded from tblLookupAppealIssues.csv

Source file: `tblLookupAppealIssues.csv`
Rows: 248

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2009-10-20 |  |
| datCreatedOn | DATE | 0.0% | 1991-06-10 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | INITIAL DEFUALT |  |
| strCode | VARCHAR | 0.0% | ?? |  |
| idnAppealIssues | INTEGER | 0.0% | 92 |  |

## lu_appeal_type

Loaded from tbllookupAppealType.csv

Source file: `tbllookupAppealType.csv`
Rows: 19

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnDDAppeal | BOOLEAN | 0.0% | false |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 5.3% | 2020-06-30 |  |
| datCreatedOn | DATE | 0.0% | 2002-11-08 |  |
| strApplDescription | VARCHAR | 0.0% | APPEAL OF MOTION TO SET ASIDE |  |
| strApplCode | VARCHAR | 0.0% | AMSA Appeal |  |
| idnAppealType | INTEGER | 0.0% | 11 |  |

## lu_application

Application type codes

Source file: `tblLookUp_Appln.csv`
Rows: 40

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 80.0% | 2017-09-30 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| APL_casetype_7 | VARCHAR | 100.0% |  |  |
| APL_casetype_6 | VARCHAR | 100.0% |  |  |
| APL_casetype_5 | VARCHAR | 95.0% | W |  |
| APL_casetype_4 | VARCHAR | 90.0% | A |  |
| APL_casetype_3 | VARCHAR | 42.5% | M |  |
| APL_casetype_2 | VARCHAR | 27.5% | E |  |
| APL_casetype_1 | VARCHAR | 15.0% | D |  |
| strAllowedDecisionCodes | VARCHAR | 0.0% | A,D,G,O,W,S,T,M,I,P |  |
| strAllowedCaseTypes | VARCHAR | 0.0% | DEP,EXC,RMV |  |
| strdescription | VARCHAR | 0.0% | NACARA ADJUSTMENT |  |
| strcode | VARCHAR | 0.0% | 202 |  |
| idnAppln | INTEGER | 0.0% | 1 |  |

## lu_bar_code_type

Loaded from tblBarCodeType.csv

Source file: `tblBarCodeType.csv`
Rows: 141

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| lngGroupID | VARCHAR | 1.4% | 200 |  |
| datModifiedOn | DATE | 73.8% | 1997-06-09 |  |
| datCreatedOn | DATE | 0.0% | 1997-06-06 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strBarCodeTypeDescription | VARCHAR | 0.0% | Tape Transmittal Bag 1 |  |
| strBarCodeTypeID | VARCHAR | 0.0% | Tape Bag 1 |  |
| idnBarCodeType | INTEGER | 0.0% | 100001 |  |

## lu_base_city

Immigration court locations

Source file: `tblLookupBaseCity.csv`
Rows: 97

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnBCIS | BOOLEAN | 0.0% | false |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 0.0% | 2020-09-22 |  |
| datCreatedOn | DATE | 0.0% | 2004-09-12 |  |
| BaseSplPrtClass | VARCHAR | 100.0% |  |  |
| RopPrtClass | VARCHAR | 100.0% |  |  |
| RopDefault | VARCHAR | 100.0% |  |  |
| I122Subf | VARCHAR | 100.0% |  |  |
| I122Sube | VARCHAR | 100.0% |  |  |
| I122Subd | VARCHAR | 100.0% |  |  |
| I122Subc | VARCHAR | 100.0% |  |  |
| I122Subb | VARCHAR | 100.0% |  |  |
| I122Suba | VARCHAR | 100.0% |  |  |
| I122Code | VARCHAR | 100.0% |  |  |
| I122Flag | VARCHAR | 100.0% |  |  |
| BaseEoir39Subf | VARCHAR | 100.0% |  |  |
| BaseEoir39Sube | VARCHAR | 100.0% |  |  |
| BaseEoir39Subd | VARCHAR | 100.0% |  |  |
| BaseEoir39Subc | VARCHAR | 100.0% |  |  |
| BaseEoir39Subb | VARCHAR | 100.0% |  |  |
| BaseEoir39Suba | VARCHAR | 100.0% |  |  |
| BaseEoir39Code | VARCHAR | 100.0% |  |  |
| BaseEoir39Flag | VARCHAR | 100.0% |  |  |
| BaseEoir36Subf | VARCHAR | 100.0% |  |  |
| BaseEoir36Sube | VARCHAR | 100.0% |  |  |
| BaseEoir36Subd | VARCHAR | 100.0% |  |  |
| BaseEoir36Subc | VARCHAR | 100.0% |  |  |
| BaseEoir36Subb | VARCHAR | 100.0% |  |  |
| BaseEoir36Suba | VARCHAR | 100.0% |  |  |
| BaseEoir36Code | VARCHAR | 100.0% |  |  |
| BaseEoir36Flag | VARCHAR | 100.0% |  |  |
| BaseEoir35Subf | VARCHAR | 100.0% |  |  |
| BaseEoir35Sube | VARCHAR | 100.0% |  |  |
| BaseEoir35Subd | VARCHAR | 100.0% |  |  |
| BaseEoir35Subc | VARCHAR | 100.0% |  |  |
| BaseEoir35Subb | VARCHAR | 100.0% |  |  |
| BaseEoir35Suba | VARCHAR | 100.0% |  |  |
| BaseEoir35Code | VARCHAR | 100.0% |  |  |
| BaseEoir35Flag | VARCHAR | 100.0% |  |  |
| BaseEoir34Subf | VARCHAR | 100.0% |  |  |
| BaseEoir34Sube | VARCHAR | 100.0% |  |  |
| BaseEoir34Subd | VARCHAR | 100.0% |  |  |
| BaseEoir34Subc | VARCHAR | 100.0% |  |  |
| BaseEoir34Subb | VARCHAR | 100.0% |  |  |
| BaseEoir34Suba | VARCHAR | 100.0% |  |  |
| BaseEoir34Code | VARCHAR | 100.0% |  |  |
| BaseEoir34Flag | VARCHAR | 100.0% |  |  |
| BaseEoir7Subf | VARCHAR | 100.0% |  |  |
| BaseEoir7Sube | VARCHAR | 100.0% |  |  |
| BaseEoir7Subd | VARCHAR | 100.0% |  |  |
| BaseEoir7Subc | VARCHAR | 100.0% |  |  |
| BaseEoir7Subb | VARCHAR | 100.0% |  |  |
| BaseEoir7Suba | VARCHAR | 100.0% |  |  |
| BaseEoir7Code | VARCHAR | 100.0% |  |  |
| BaseEoir7Flag | VARCHAR | 100.0% |  |  |
| BaseEoir6Subf | VARCHAR | 100.0% |  |  |
| BaseEoir6Sube | VARCHAR | 100.0% |  |  |
| BaseEoir6Subd | VARCHAR | 100.0% |  |  |
| BaseEoir6Subc | VARCHAR | 100.0% |  |  |
| BaseEoir6Subb | VARCHAR | 100.0% |  |  |
| BaseEoir6Suba | VARCHAR | 100.0% |  |  |
| BaseEoir6Code | VARCHAR | 100.0% |  |  |
| BaseEoir6Flag | VARCHAR | 100.0% |  |  |
| BaseEoir5Subf | VARCHAR | 100.0% |  |  |
| BaseEoir5Sube | VARCHAR | 100.0% |  |  |
| BaseEoir5Subd | VARCHAR | 100.0% |  |  |
| BaseEoir5Subc | VARCHAR | 100.0% |  |  |
| BaseEoir5Subb | VARCHAR | 100.0% |  |  |
| BaseEoir5Suba | VARCHAR | 100.0% |  |  |
| BaseEoir5Code | VARCHAR | 100.0% |  |  |
| BaseEoir5Flag | VARCHAR | 100.0% |  |  |
| BaseEoir2Subf | VARCHAR | 100.0% |  |  |
| BaseEoir2Sube | VARCHAR | 100.0% |  |  |
| BaseEoir2Subd | VARCHAR | 100.0% |  |  |
| BaseEoir2Subc | VARCHAR | 100.0% |  |  |
| BaseEoir2Subb | VARCHAR | 100.0% |  |  |
| BaseEoir2Suba | VARCHAR | 100.0% |  |  |
| BaseEoir2Code | VARCHAR | 100.0% |  |  |
| BaseEoir2Flag | VARCHAR | 100.0% |  |  |
| BaseEoir1Subf | VARCHAR | 100.0% |  |  |
| BaseEoir1Sube | VARCHAR | 100.0% |  |  |
| BaseEoir1Subd | VARCHAR | 100.0% |  |  |
| BaseEoir1Subc | VARCHAR | 100.0% |  |  |
| BaseEoir1Subb | VARCHAR | 100.0% |  |  |
| BaseEoir1Suba | VARCHAR | 100.0% |  |  |
| BaseEoir1Code | VARCHAR | 100.0% |  |  |
| BaseEoir1Flag | VARCHAR | 100.0% |  |  |
| strRegion | VARCHAR | 0.0% | Western |  |
| BASE_PHONE_NO | VARCHAR | 0.0% | 6714727528 |  |
| BASE_ZIP_2 | VARCHAR | 41.2% | 0 |  |
| BASE_ZIP_1 | VARCHAR | 0.0% | 96910 |  |
| BASE_STATE | VARCHAR | 0.0% | GU |  |
| BASE_CITY | VARCHAR | 0.0% | HAGATNA |  |
| BASE_ST_ADDRESS | VARCHAR | 0.0% | 203 ASPINALL AVENUE |  |
| BaseAutomated | VARCHAR | 21.6% | M |  |
| BASE_CITY_NAME | VARCHAR | 0.0% | IMMIGRATION COURT |  |
| BASE_CITY_CODE | VARCHAR | 0.0% | AGA | Joins to lu_base_city.BASE_CITY_CODE |
| idnBaseCity | INTEGER | 0.0% | 2 |  |

## lu_bia

Loaded from tblLookupBIA.csv

Source file: `tblLookupBIA.csv`
Rows: 895

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnSLStatus | BOOLEAN | 0.0% | true |  |
| strPanel | VARCHAR | 11.4% | 0 |  |
| strTeam | VARCHAR | 8.3% | w |  |
| datModifiedOn | DATE | 0.1% | 2021-10-02 |  |
| datCreatedOn | DATE | 0.0% | 2019-06-20 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strGroup | VARCHAR | 0.0% | co |  |
| strCode | VARCHAR | 0.0% | *** |  |
| idnBIA | INTEGER | 0.0% | 669 |  |

## lu_bia_decision

BIA decision codes

Source file: `tblLookupBIADecision.csv`
Rows: 55

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 12.7% | 2008-09-18 |  |
| datCreatedOn | DATE | 0.0% | 1996-07-12 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | American Baptist Church Settlement |  |
| strCode | VARCHAR | 0.0% | ABC |  |
| idnBIAdec | INTEGER | 0.0% | 3 |  |

## lu_biacluster

Loaded from tblLookup_BIACluster.csv

Source file: `tblLookup_BIACluster.csv`
Rows: 244

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| Sort_End | VARCHAR | 0.0% | 999999999 |  |
| Sort_Start | VARCHAR | 0.0% | 000000000 |  |
| idnBaseCity | INTEGER | 0.0% | 75 |  |
| Cluster_Subgroup | VARCHAR | 100.0% |  |  |
| Cluster_Name | VARCHAR | 0.0% | Cluster 3 |  |
| Region_Name | VARCHAR | 66.0% | WEST |  |
| Report_Classification | VARCHAR | 0.0% | MOTIONS |  |
| idnLookupCluster | INTEGER | 0.0% | 906 |  |

## lu_biadecision_type

Loaded from tblLookupBIADecisionType.csv

Source file: `tblLookupBIADecisionType.csv`
Rows: 5

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 80.0% | 2002-05-07 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | American Baptist Church Settlement |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnBIAdecType | INTEGER | 0.0% | 1 |  |

## lu_board_motions

Loaded from tblLookupBoardMotions.csv

Source file: `tblLookupBoardMotions.csv`
Rows: 18

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| intMandatoryIJDecisionByDays | VARCHAR | 0.0% | 10 |  |
| intMandatoryResponseDueByDays | VARCHAR | 0.0% | 30 |  |
| strAllowedDecisions | VARCHAR | 0.0% | GRN,DEN |  |
| strHelp | VARCHAR | 5.6% | AAF Non-Payment Order |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 22.2% | 2026-07-29 |  |
| datCreatedOn | DATE | 0.0% | 2026-07-29 |  |
| strBoardMotionDescription | VARCHAR | 0.0% | AAF Non-Payment Order |  |
| strBoardMotionCode | VARCHAR | 0.0% | AN |  |
| idnBoardMotion | INTEGER | 0.0% | 26 |  |

## lu_br_deduct_codes

Loaded from tbl_br_DeductCodes.csv

Source file: `tbl_br_DeductCodes.csv`
Rows: 15

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| idnRepl | INTEGER | 0.0% | 1 |  |
| DeductDescription | VARCHAR | 0.0% | Late arrival at window, present in Courtroom |  |
| DeductCode | VARCHAR | 13.3% | D1 |  |

## lu_cal_type

Calendar type codes

Source file: `tbllookupCal_Type.csv`
Rows: 29

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 96.6% | 2021-01-22 |  |
| datCreatedOn | DATE | 0.0% | 2002-11-21 |  |
| strCalTypeDescription | VARCHAR | 0.0% | Non-Work |  |
| strCalTypeCode | VARCHAR | 0.0% | * |  |
| idnCalType | INTEGER | 0.0% | 1 |  |

## lu_callup_reasons

Loaded from tblLookupCallup_Reasons.csv

Source file: `tblLookupCallup_Reasons.csv`
Rows: 30

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 36.7% | 2021-05-14 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| Regd_By | VARCHAR | 0.0% | A |  |
| strDesciption | VARCHAR | 0.0% | Respondent/Attorney/Representative to file brief (not appeal) |  |
| strcode | VARCHAR | 0.0% | AB |  |
| idnCalTypeReason | INTEGER | 0.0% | 1 |  |

## lu_case_identifier

Loaded from tblLookUpCaseIdentifier.csv

Source file: `tblLookUpCaseIdentifier.csv`
Rows: 122

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 4.1% | 2005-06-20 |  |
| datCreatedOn | DATE | 0.0% | 2005-06-20 |  |
| strDescription | VARCHAR | 0.0% | NTA not filed within 120 days of EPRD |  |
| strcode | VARCHAR | 0.0% | 4M |  |
| idnCaseIdentifier | INTEGER | 0.0% | 63 |  |

## lu_case_priority

Loaded from tblLookup_CasePriority.csv

Source file: `tblLookup_CasePriority.csv`
Rows: 6

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnBypasscal | BOOLEAN | 0.0% | true |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| DATMODIFIEDON | DATE | 0.0% | 2017-03-10 |  |
| DATCREATEDON | DATE | 0.0% | 2014-08-05 |  |
| strDays | VARCHAR | 0.0% | 28 |  |
| strDescription | VARCHAR | 0.0% | Adults With Children-Release on ATD |  |
| strCode | VARCHAR | 0.0% | AWC/ATD |  |
| idnCasePriority | INTEGER | 0.0% | 2 |  |

## lu_case_type

Case type codes

Source file: `tblLookupCaseType.csv`
Rows: 13

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strAnsirCode | VARCHAR | 30.8% | A |  |
| datModifiedOn | DATE | 53.8% | 1998-10-31 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Asylum Only Case |  |
| strCode | VARCHAR | 0.0% | AOC |  |
| idnCaseType | INTEGER | 0.0% | 2 |  |

## lu_category

Loaded from tbllookup_Category.csv

Source file: `tbllookup_Category.csv`
Rows: 7

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| idnCategory | INTEGER | 0.0% | 1 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 71.4% | 2003-10-20 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strDescription | VARCHAR | 0.0% | 235(c) Security |  |
| strcategory | VARCHAR | 0.0% | 1 |  |

## lu_charges

Charge codes and descriptions

Source file: `tbllookupCharges.csv`
Rows: 252

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| DatModifiedOn | DATE | 85.3% | 2005-02-14 |  |
| DatCreatedOn | DATE | 0.0% | 2005-02-14 |  |
| aggravate_felon | BOOLEAN | 0.0% | false |  |
| criminal_flag | BOOLEAN | 0.0% | false |  |
| applicable_case_type3 | VARCHAR | 97.6% | M |  |
| applicable_case_type2 | VARCHAR | 64.3% | R |  |
| applicable_case_type1 | VARCHAR | 0.8% | d |  |
| strCodeDescription | VARCHAR | 22.6% | Commission of Acts of Torture or Extrajudicial Killings |  |
| strCodeString | VARCHAR | 0.0% | (212)(a)(03)(E)(i  )(   ) |  |
| strCode | VARCHAR | 0.0% | (212)(a)(03)(E)(i  )(   ) |  |
| idnCharges | INTEGER | 0.0% | 234 |  |

## lu_claimed_issue

Loaded from tblLookupClaimedIssue.csv

Source file: `tblLookupClaimedIssue.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2002-11-17 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Asylee |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnClaimedIssue | INTEGER | 0.0% | 2 |  |

## lu_comment_type_board

Loaded from tblLookupCommentTypeBoard.csv

Source file: `tblLookupCommentTypeBoard.csv`
Rows: 9

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strDescription | VARCHAR | 0.0% | APU Screen |  |
| strcode | VARCHAR | 0.0% | APU |  |
| idnCommBrd | INTEGER | 0.0% | 1 |  |

## lu_components

Loaded from tblLookupComponents.csv

Source file: `tblLookupComponents.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strCompDescription | VARCHAR | 0.0% | Board |  |
| strCompCode | VARCHAR | 0.0% | B |  |
| idnComp | INTEGER | 0.0% | 2 |  |

## lu_consulted_with

Loaded from tblLookupConsultedWith.csv

Source file: `tblLookupConsultedWith.csv`
Rows: 5

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2002-11-17 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Attorney |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnConsultedWith | INTEGER | 0.0% | 2 |  |

## lu_contact_change

Loaded from tblLookupContactChange.csv

Source file: `tblLookupContactChange.csv`
Rows: 9

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 11.1% | 2017-04-12 |  |
| datCreatedOn | DATE | 0.0% | 2017-04-12 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strContactChangeDesc | VARCHAR | 0.0% | New EOIR 26 Form - Board |  |
| strContactChangeCode | VARCHAR | 0.0% | 26-BOARD |  |
| idnContactChange | INTEGER | 0.0% | 6 |  |

## lu_country

Loaded from tblLookupCountry.csv

Source file: `tblLookupCountry.csv`
Rows: 254

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.8% | 2019-10-05 |  |
| datCreatedOn | DATE | 0.0% | 2010-07-16 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | UNKNOWN NATIONALITY |  |
| strCode | VARCHAR | 0.0% | ?? |  |
| idnCountry | INTEGER | 0.0% | 252 |  |

## lu_court_decision

Court decision codes (by case type)

Source file: `tblLookupCourtDecision.csv`
Rows: 217

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 60.8% | 2004-01-16 |  |
| datCreatedOn | DATE | 21.7% | 2002-11-30 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strFinalDisposition | VARCHAR | 100.0% |  |  |
| strDecType | VARCHAR | 0.0% | C |  |
| strDecDescription | VARCHAR | 0.0% | Abandonment |  |
| strDecCode | VARCHAR | 0.0% | A |  |
| strCaseType | VARCHAR | 0.0% | AOC |  |
| idnCourtDecision | INTEGER | 0.0% | 69 |  |

## lu_custody_status

Custody status codes

Source file: `tblLookupCustodyStatus.csv`
Rows: 3

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 66.7% | 2002-05-07 |  |
| datCreatedOn | DATE | 100.0% |  |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Detained |  |
| strSelecBoxVal | VARCHAR | 0.0% | D |  |
| strCode | VARCHAR | 0.0% | D |  |
| idnCustodyStatus | INTEGER | 0.0% | 2 |  |

## lu_dddecision_types

Loaded from tblLookUpDDDecisionTypes.csv

Source file: `tblLookUpDDDecisionTypes.csv`
Rows: 23

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strFinalDisp | VARCHAR | 56.5% | Z |  |
| strDecsription | VARCHAR | 100.0% |  |  |
| datModifiedOn | DATE | 69.6% | 2018-10-13 |  |
| datCreatedOn | DATE | 0.0% | 2018-10-13 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | ROP Returned to Court to correct deficiency |  |
| strCode | VARCHAR | 0.0% | ACR |  |
| idnDDDecType | INTEGER | 0.0% | 23 |  |

## lu_dec_code

Loaded from tblDecCode.csv

Source file: `tblDecCode.csv`
Rows: 12

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 91.7% | 2003-11-03 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| strDescription | VARCHAR | 0.0% | LEGALLY ADMITTED |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnDecCode | INTEGER | 0.0% | 1 |  |

## lu_district_location

Loaded from tblLookupDistrictLocation.csv

Source file: `tblLookupDistrictLocation.csv`
Rows: 289

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2021-05-04 |  |
| datCreatedOn | DATE | 0.0% | 1996-12-04 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | ARLINGTON ASYLUM OFFICE, ARLINGTON |  |
| strCode | VARCHAR | 0.0% | AAO |  |
| idnDestLoc | INTEGER | 0.0% | 11 |  |

## lu_fed_court_decision

Loaded from tblLookupFedCourtDecision.csv

Source file: `tblLookupFedCourtDecision.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2002-05-07 |  |
| datCreatedOn | DATE | 0.0% | 1996-10-30 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Dismiss |  |
| strCode | VARCHAR | 0.0% | DIS |  |
| idnFedCourt | INTEGER | 0.0% | 1 |  |

## lu_fed_court_remand_to

Loaded from tblLookupFedCourtRemandTo.csv

Source file: `tblLookupFedCourtRemandTo.csv`
Rows: 2

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 50.0% | 2002-05-07 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Remand to BIA |  |
| strCode | VARCHAR | 0.0% | BIA |  |
| idnFedCourtRem | INTEGER | 0.0% | 1 |  |

## lu_fee_status

Loaded from tblLookupFeeStatus.csv

Source file: `tblLookupFeeStatus.csv`
Rows: 5

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 40.0% | 1996-12-04 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | Waiver Request Denied |  |
| strCode | VARCHAR | 0.0% | D |  |
| idnFee | INTEGER | 0.0% | 1 |  |

## lu_fee_type

Loaded from tblLookupFeeType.csv

Source file: `tblLookupFeeType.csv`
Rows: 3

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| curAmount | VARCHAR | 0.0% | 50.0000 |  |
| strDescription | VARCHAR | 0.0% | 212 Application |  |
| strCode | VARCHAR | 0.0% | 212 Application |  |
| idnFeeType | INTEGER | 0.0% | 1 |  |

## lu_filed_by

Loaded from tblLookupFiledBy.csv

Source file: `tblLookupFiledBy.csv`
Rows: 9

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 22.2% | 2004-06-28 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Alien |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnFiled | INTEGER | 0.0% | 1 |  |

## lu_filing_method

Loaded from tblLookupFiling_Method.csv

Source file: `tblLookupFiling_Method.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 50.0% | 2004-01-31 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strFilingMethodDesc | VARCHAR | 0.0% | EOIR Form 6 |  |
| strFilingMethodCode | VARCHAR | 0.0% | 6 |  |
| idnFilingMethod | INTEGER | 0.0% | 3 |  |

## lu_filing_method_party

Loaded from tblLookupFiling_Method_Party.csv

Source file: `tblLookupFiling_Method_Party.csv`
Rows: 10

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 60.0% | 2017-12-02 |  |
| datCreatedOn | DATE | 0.0% | 2002-11-05 |  |
| strFilingPartyDesc | VARCHAR | 0.0% | Attorney / Alien |  |
| strFilingPartyCode | VARCHAR | 0.0% | A |  |
| idnFilingParty | INTEGER | 0.0% | 1 |  |

## lu_final_disposition

Loaded from tblLookupFinalDisposition.csv

Source file: `tblLookupFinalDisposition.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 50.0% | 1996-06-24 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Alien not deported or excluded |  |
| strCode | VARCHAR | 0.0% | N |  |
| idnFinDisp | INTEGER | 0.0% | 2 |  |

## lu_hearing_location

Hearing location codes

Source file: `tblLookupHloc.csv`
Rows: 1,381

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnPrnRopOnAdd | BOOLEAN | 0.0% | false |  |
| blnPrnWksOnAdd | BOOLEAN | 0.0% | false |  |
| CircuitCourt | VARCHAR | 0.0% | 9 |  |
| intCalCDRDays | VARCHAR | 0.0% | 0 |  |
| intCalCFRDays | VARCHAR | 0.0% | 1 |  |
| intCalEOIRDays | VARCHAR | 0.0% | 1 |  |
| intCalINSDays | VARCHAR | 0.0% | 1 |  |
| intCalINSAsyMDays | VARCHAR | 0.0% | 1 |  |
| intCalINSAsyPDays | VARCHAR | 0.0% | 1 |  |
| intCalDetainDays | VARCHAR | 0.0% | 0 |  |
| ROPLoc | VARCHAR | 0.0% | TAC |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 0.0% | 2025-01-16 |  |
| datCreatedOn | DATE | 19.6% | 2024-11-07 |  |
| Hear_Active_Flg | VARCHAR | 0.0% | Y |  |
| Hear_Det_Type | VARCHAR | 77.7% | F |  |
| Hear_Trans_COV_City | VARCHAR | 0.7% | CAF |  |
| Hear_Admin_Hear_Loc | VARCHAR | 0.0% | TAC |  |
| HEARING_BASE_CITY | VARCHAR | 0.0% | TAC |  |
| HEARING_PHONE_NO | VARCHAR | 0.0% | 2537796020 |  |
| HEARING_ZIP | VARCHAR | 0.0% | 98421 |  |
| HEARING_STATE | VARCHAR | 0.0% | WA |  |
| HEARING_CITY | VARCHAR | 0.0% | TACOMA |  |
| HEARING_ST_ADDRESS | VARCHAR | 0.0% | 1623 EAST J STREET, SUITE 3 |  |
| HEARING_CORR_TITLE | VARCHAR | 0.0% | IMMIGRATION COURT |  |
| HEARING_LOC_NAME | VARCHAR | 0.0% | TACOMA - CFR/RFR |  |
| HEARING_LOC_CODE | VARCHAR | 0.0% | 1AC | Joins to lu_hearing_location.LOC_CODE |
| idnHloc | INTEGER | 0.0% | 1045 |  |

## lu_hold_reason

Loaded from tblLookupHoldReason.csv

Source file: `tblLookupHoldReason.csv`
Rows: 37

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| idnRepl | INTEGER | 0.0% | 13 |  |
| datModifiedOn | DATE | 0.0% | 2003-03-24 |  |
| datCreatedOn | DATE | 0.0% | 2002-01-18 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | St. Cyr Not Guilty-Pending Reg |  |
| strCode | VARCHAR | 0.0% | St. Cyr Not Guilty |  |

## lu_holidays

Loaded from tblLookupHolidays.csv

Source file: `tblLookupHolidays.csv`
Rows: 613

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 91.7% | 2005-12-05 |  |
| datCreatedOn | DATE | 0.0% | 2003-01-28 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| Holiday_Day | VARCHAR | 67.0% | TUESDAY |  |
| strHoliday | VARCHAR | 0.0% | New Year's |  |
| datHoliday | DATE | 0.0% | 1990-01-01 |  |
| idnHoliday | INTEGER | 0.0% | 173 |  |

## lu_in_tr_pr

Loaded from tblLookupInTrPr.csv

Source file: `tblLookupInTrPr.csv`
Rows: 94

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 70.2% | 2003-07-30 |  |
| datCreatedOn | DATE | 0.0% | 2003-05-14 |  |
| IntrHloc | VARCHAR | 0.0% | TUC |  |
| Base_city | VARCHAR | 0.0% | TUC |  |
| IntrCode | VARCHAR | 0.0% | 001 |  |
| Inhouse_Flag | VARCHAR | 0.0% | Y |  |
| IDNIntrpr | INTEGER | 0.0% | 1 |  |

## lu_in_tr_pr_agenda

Loaded from tblLookupInTrPrAgenda.csv

Source file: `tblLookupInTrPrAgenda.csv`
Rows: 5

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 0.0% | 2004-07-12 |  |
| datCreatedOn | DATE | 0.0% | 2004-07-12 |  |
| BreakEndTime | VARCHAR | 40.0% | 1130 |  |
| BreakStartTime | VARCHAR | 40.0% | 1100 |  |
| EndTime | VARCHAR | 0.0% | 1130 |  |
| StartTime | VARCHAR | 0.0% | 0800 |  |
| IntrHloc | VARCHAR | 0.0% | BCM |  |
| LanguageCode | VARCHAR | 0.0% | FR |  |
| LanguageWorkday | VARCHAR | 0.0% | 1 |  |
| Base_city | VARCHAR | 0.0% | BCM |  |
| IntrCode | VARCHAR | 0.0% | DB |  |
| idnIntrPrAgenda | INTEGER | 0.0% | 362 |  |

## lu_in_tr_pr_lang_group

Loaded from tblLookupInTrPrLangGroup.csv

Source file: `tblLookupInTrPrLangGroup.csv`
Rows: 2

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATMODIFIEDON | DATE | 0.0% | 2004-07-12 |  |
| DATCREATEDON | DATE | 0.0% | 2004-07-12 |  |
| IndivGroupFlag | VARCHAR | 0.0% | Y |  |
| MasterGroupFlag | VARCHAR | 0.0% | Y |  |
| Base_city | VARCHAR | 0.0% | WIK |  |
| LanguageCode | VARCHAR | 0.0% | PAL |  |
| idnIntrLangGroup | INTEGER | 0.0% | 32 |  |

## lu_inmate

Loaded from tblLookupInmate.csv

Source file: `tblLookupInmate.csv`
Rows: 4,850

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 5.7% | 2007-01-13 |  |
| datCreatedOn | DATE | 0.0% | 2007-01-13 |  |
| INMATE_PHONE | VARCHAR | 53.9% | 1111111111 |  |
| INMATE_ZIP | VARCHAR | 0.1% | 969320000 |  |
| INMATE_ST | VARCHAR | 0.5% | GU |  |
| INMATE_CITY | VARCHAR | 0.5% | HAGATNA |  |
| INMATE_ADDRESS | VARCHAR | 0.5% | P.O. BOX 3236 |  |
| INMATE_NAME | VARCHAR | 0.0% | DEPARTMENT OF CORRECTIONS |  |
| INMATE_CODE | VARCHAR | 0.0% | GUA |  |
| INMATE_BASE | VARCHAR | 0.0% | AGA |  |
| ID | VARCHAR | 0.0% | 1 |  |

## lu_inmate_housing

Loaded from tblInmateHousing.csv

Source file: `tblInmateHousing.csv`
Rows: 4,752

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.9% | true |  |
| datModifiedOn | DATE | 83.6% | 2009-04-25 |  |
| datCreatedOn | DATE | 0.9% | 2003-08-10 |  |
| Strdescription | VARCHAR | 0.5% | DEPARTMENT OF CORRECTIONS |  |
| Strcode | VARCHAR | 0.0% | GUA |  |
| Strcity | VARCHAR | 0.4% | AGA |  |
| idnInmate | INTEGER | 0.5% | 1 |  |

## lu_ins_clock_entry_status

Loaded from tblLookup_INS_Clock_Entry_Status.csv

Source file: `tblLookup_INS_Clock_Entry_Status.csv`
Rows: 8

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 50.0% | 2018-08-17 |  |
| datCreatedOn | DATE | 0.0% | 2003-02-05 |  |
| INS_Clock_Entry_Status_Desc | VARCHAR | 0.0% | ABC case - No Clock |  |
| INS_Clock_Entry_Status_Code | VARCHAR | 0.0% | A |  |
| idnClEnSt | INTEGER | 0.0% | 1 |  |

## lu_insaddress

Loaded from tblLookUpINSAddress.csv

Source file: `tblLookUpINSAddress.csv`
Rows: 1,469

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 0.0% | 2026-08-07 |  |
| datCreatedOn | DATE | 0.0% | 2026-08-07 |  |
| strZip | VARCHAR | 0.0% | 98421 |  |
| strState | VARCHAR | 0.0% | WA |  |
| strCity | VARCHAR | 0.0% | TACOMA |  |
| strAddress | VARCHAR | 0.0% | 1623 EAST J STREET, STE. 2 |  |
| strName | VARCHAR | 0.0% | DHS/ICE OFFICE OF CHIEF COUNSEL - TAC |  |
| strCode | VARCHAR | 0.0% | 1AC |  |
| idnLookupINSAddress | INTEGER | 0.0% | 1023 |  |

## lu_insloc

Loaded from tblLookupINSLOC.csv

Source file: `tblLookupINSLOC.csv`
Rows: 1,020

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 97.0% | 2023-02-23 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| INSLOC_ASY | VARCHAR | 0.0% | N |  |
| INSLOC_INFO | VARCHAR | 0.0% | ARLINGTON ASYLUM OFFICE, ARLINGTON |  |
| INSLOC_CODE | VARCHAR | 0.0% | AAO |  |
| ID | VARCHAR | 0.0% | 783 |  |

## lu_insoffice

Loaded from tblINSoffice.csv

Source file: `tblINSoffice.csv`
Rows: 74

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 9.5% | 2004-10-25 |  |
| datCreatedOn | DATE | 0.0% | 2004-10-12 |  |
| strDescription | VARCHAR | 0.0% | TESTTEST |  |
| strCode | VARCHAR | 0.0% | AAA |  |
| idnINSOffice | INTEGER | 0.0% | 9 |  |

## lu_insta

Loaded from tblLookupINSTA.csv

Source file: `tblLookupINSTA.csv`
Rows: 9,671

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 1.2% | 2026-09-09 |  |
| datCreatedOn | DATE | 0.0% | 2026-09-09 |  |
| INSTA_BASE | VARCHAR | 0.0% | ADL |  |
| ID | VARCHAR | 0.0% | 12303 |  |

## lu_intr_pr_lang

Loaded from tblLookupIntrPrLang.csv

Source file: `tblLookupIntrPrLang.csv`
Rows: 14

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATMODIFIEDON | DATE | 0.0% | 2004-07-12 |  |
| DATCREATEDON | DATE | 0.0% | 2004-07-12 |  |
| LanguageCode | VARCHAR | 0.0% | AFR |  |
| Base_city | VARCHAR | 0.0% | BAL |  |
| IntrCode | VARCHAR | 0.0% | BAL |  |
| idnintrprLang | INTEGER | 0.0% | 112 |  |

## lu_judge

Immigration judge codes

Source file: `tblLookupJudge.csv`
Rows: 1,840

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| intOrderMM | VARCHAR | 0.0% | 505 |  |
| intOrderMA | VARCHAR | 0.0% | 507 |  |
| blnLastOnWheelMA | BOOLEAN | 0.0% | false |  |
| blnSkippedOnWheelMA | BOOLEAN | 0.0% | true |  |
| blnLastOnWheel | BOOLEAN | 0.0% | false |  |
| blnSkippedOnWheel | BOOLEAN | 0.0% | true |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 10.3% | 2011-02-10 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| JUDGE_PHONE_NO | VARCHAR | 100.0% |  |  |
| JUDGE_ZIP_2 | VARCHAR | 100.0% |  |  |
| JUDGE_ZIP_1 | VARCHAR | 100.0% |  |  |
| JUDGE_STATE | VARCHAR | 100.0% |  |  |
| JUDGE_CITY | VARCHAR | 100.0% |  |  |
| JUDGE_ST_ADDRESS | VARCHAR | 100.0% |  |  |
| JUDGE_NAME | VARCHAR | 0.0% | Atenaide, Anthony |  |
| JUDGE_CODE | VARCHAR | 0.0% | AA |  |
| idnJudge | INTEGER | 0.0% | 126 |  |

## lu_judge_base_city

Loaded from tblLookupJudgeBaseCity.csv

Source file: `tblLookupJudgeBaseCity.csv`
Rows: 12,777

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 0.0% | 2018-09-12 |  |
| datCreatedOn | DATE | 0.0% | 2018-09-12 |  |
| JUMODTIME | VARCHAR | 99.2% | 1625 |  |
| JUMODDATE | VARCHAR | 99.2% | 2003-07-07 00:00:00.000 |  |
| JUINTIME | VARCHAR | 99.2% | 1145 |  |
| JUINDATE | VARCHAR | 99.2% | 2003-05-01 00:00:00.000 |  |
| JUCOURTROOM4 | VARCHAR | 100.0% | COURTROOM #3 |  |
| JUCOURTROOM3 | VARCHAR | 99.9% | CHICAGO, IL. 60605 |  |
| JUCOURTROOM2 | VARCHAR | 99.8% | COURTROOM 4 |  |
| JUCOURTROOM | VARCHAR | 98.1% | COURTROOM 4 |  |
| JULOC4 | VARCHAR | 100.0% | WIS |  |
| JULOC3 | VARCHAR | 99.8% | LGD |  |
| JULOC2 | VARCHAR | 99.7% | WAD |  |
| JULOC | VARCHAR | 98.1% | WAS |  |
| JUTIME | VARCHAR | 89.6% | 1108 |  |
| JUDATE | VARCHAR | 89.5% | 1999-12-01 00:00:00.000 |  |
| JUBOOKFACT | VARCHAR | 88.7% | 02 |  |
| JUDGE_PHONE_NO | VARCHAR | 88.8% | 6714727528 |  |
| JUDGE_ZIP | VARCHAR | 88.7% | 969100000 |  |
| JUDGE_STATE | VARCHAR | 88.7% | GU |  |
| JUDGE_CITY | VARCHAR | 88.1% | UNK |  |
| JUDGE_ST_ADDRESS | VARCHAR | 88.0% | UNK |  |
| JUDGE_NAME | VARCHAR | 0.0% | KHAN, AMIENA A. |  |
| JUDGE_CODE | VARCHAR | 0.0% | AAK |  |
| BASE_CITY_CODE | VARCHAR | 0.0% | ADL | Joins to lu_base_city.BASE_CITY_CODE |
| idnJudgeCity | INTEGER | 0.0% | 7254 |  |

## lu_juvenile

Loaded from tblLookup_Juvenile.csv

Source file: `tblLookup_Juvenile.csv`
Rows: 6

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATMODIFIEDON | DATE | 0.0% | 2017-04-07 |  |
| DATCREATEDON | DATE | 0.0% | 2014-09-06 |  |
| strDescription | VARCHAR | 0.0% | Juvenile Case |  |
| strCode | VARCHAR | 0.0% | J |  |
| idnJuvenile | INTEGER | 0.0% | 1 |  |

## lu_language

Language codes

Source file: `tblLanguage.csv`
Rows: 743

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 1.7% | 2008-02-19 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| strDescription | VARCHAR | 0.0% | BYELORUSSIAN |  |
| strCode | VARCHAR | 0.0% | BYE |  |
| idnLang | INTEGER | 0.0% | 48 |  |

## lu_motion_type

Motion type codes

Source file: `tblLookupMotionType.csv`
Rows: 72

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| IntMandatoryResponseDueByDaysDefault | VARCHAR | 0.0% | 10 |  |
| blnAsylClockAffectedStart | BOOLEAN | 0.0% | false |  |
| blnAsylClockAffected | BOOLEAN | 0.0% | false |  |
| blnCertServiceReqd | BOOLEAN | 0.0% | false |  |
| blnIJGrantPriorToHearing | BOOLEAN | 0.0% | false |  |
| blnAdminCompleteReq | BOOLEAN | 0.0% | false |  |
| blnHearingRequiredtoFile | BOOLEAN | 0.0% | false |  |
| blnFileOrally | BOOLEAN | 0.0% | false |  |
| blnFeeRequired | BOOLEAN | 0.0% | false |  |
| intMandatoryIJDecisionByDays | VARCHAR | 0.0% | 0 |  |
| blnFileAfterIJDecision | BOOLEAN | 0.0% | false |  |
| blnFilePriortoIJDecision | BOOLEAN | 0.0% | true |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 43.1% | 2004-03-26 |  |
| datCreatedOn | DATE | 65.3% | 2003-08-04 |  |
| strLegacyCode | VARCHAR | 69.4% | B |  |
| strFilingPartyCodes | VARCHAR | 0.0% | ALL |  |
| strMotionSummary | VARCHAR | 98.6% | Bond record |  |
| strMotionDesc | VARCHAR | 0.0% | Motion for Administrative Closure |  |
| strMotionType | VARCHAR | 0.0% | M |  |
| strMotionCode | VARCHAR | 0.0% | AC |  |
| idnMotionType | INTEGER | 0.0% | 1 |  |

## lu_nationality

Nationality/country codes

Source file: `tblLookupNationality.csv`
Rows: 251

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 94.4% | 2019-08-05 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| NAT_NACARA_COUNTRY | VARCHAR | 0.0% | N |  |
| NAT_COUNTRY_NAME | VARCHAR | 0.0% | UNKNOWN COUNTRY |  |
| NAT_NAME | VARCHAR | 0.0% | UNKNOWN NATIONALITY |  |
| NAT_CODE | VARCHAR | 0.0% | ?? |  |
| ID | VARCHAR | 0.0% | 1 |  |

## lu_notice

Loaded from tblLookupNOTICE.csv

Source file: `tblLookupNOTICE.csv`
Rows: 598

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strNoticeOrder | VARCHAR | 0.0% | N |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| datModifiedOn | DATE | 58.9% | 2005-10-08 |  |
| datCreatedOn | DATE | 95.0% | 2023-07-07 |  |
| Notice_Disp | VARCHAR | 0.0% | RELIEF FROM EXCLUSION ABANDONED - DEPORT |  |
| Notice_Code | VARCHAR | 0.0% | #Q |  |
| IDNNOTORD | INTEGER | 0.0% | 1 |  |

## lu_oaresponse

Loaded from tblLookupOAResponse.csv

Source file: `tblLookupOAResponse.csv`
Rows: 4

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 25.0% | 2004-10-19 |  |
| datCreatedOn | DATE | 0.0% | 1996-06-24 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | NO SHOW |  |
| strCode | VARCHAR | 0.0% | N |  |
| idnOAResp | INTEGER | 0.0% | 1 |  |

## lu_office_location

Loaded from tblLookupOfficeLocation.csv

Source file: `tblLookupOfficeLocation.csv`
Rows: 265

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2025-06-11 |  |
| datCreatedOn | DATE | 0.0% | 2025-06-11 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | DHS/CIS - ALBUQUERQUE FIELD OFFICE, NM |  |
| strCode | VARCHAR | 0.0% | ABQ |  |
| idnOffLoc | INTEGER | 0.0% | 263 |  |

## lu_panels

Loaded from tblPanels.csv

Source file: `tblPanels.csv`
Rows: 20

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 20.0% | 2023-03-21 |  |
| datCreatedOn | DATE | 30.0% | 2001-09-12 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strDescription | VARCHAR | 0.0% | Panel #6 |  |
| strCode | VARCHAR | 0.0% | 6 |  |
| idnPanel | INTEGER | 0.0% | 1 |  |

## lu_priority

Loaded from tblLookupPriority.csv

Source file: `tblLookupPriority.csv`
Rows: 16

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 12.5% | 2002-05-07 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDays | VARCHAR | 0.0% | 7 |  |
| strDescription | VARCHAR | 0.0% | Asylum |  |
| strCode | VARCHAR | 0.0% | Asylum |  |
| idnPriority | INTEGER | 0.0% | 1 |  |

## lu_region

Loaded from tblLookupRegion.csv

Source file: `tblLookupRegion.csv`
Rows: 2

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-06-25 |  |
| strDescription | VARCHAR | 0.0% | Eastern |  |
| strcode | VARCHAR | 0.0% | Eastern |  |
| idnRegion | INTEGER | 0.0% | 1 |  |

## lu_responsible_party

Loaded from tblResponsibleParty.csv

Source file: `tblResponsibleParty.csv`
Rows: 3,381

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| lngLocationTypeID | VARCHAR | 0.0% | 100529 |  |
| datModifiedOn | DATE | 2.5% | 2012-05-30 |  |
| datCreatedOn | DATE | 0.0% | 2011-11-14 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| strResponsiblePartyDescription | VARCHAR | 99.3% | Test Audit Responsible Party |  |
| idnResponsibleParty | INTEGER | 0.0% | 103132 |  |

## lu_schedule_type

Schedule type codes

Source file: `tbllookupSchedule_Type.csv`
Rows: 36

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnSubsequent | BOOLEAN | 0.0% | true |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strSchedType | VARCHAR | 0.0% | N |  |
| datModifiedOn | DATE | 83.3% | 2007-10-18 |  |
| datCreatedOn | DATE | 19.4% | 2002-11-21 |  |
| strDescription | VARCHAR | 0.0% | Non-Work |  |
| strCode | VARCHAR | 0.0% | ** |  |
| idnSchType | INTEGER | 0.0% | 1 |  |

## lu_service_method

Loaded from tblLookupService_Method.csv

Source file: `tblLookupService_Method.csv`
Rows: 6

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 83.3% | 2023-01-25 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strCertificateOfServiceMethDesc | VARCHAR | 0.0% | CERTIFIED MAIL |  |
| strCertificateOfServiceMethCode | VARCHAR | 0.0% | C |  |
| idnCertificateOfServiceMeth | INTEGER | 0.0% | 2 |  |

## lu_sex

Loaded from tblLookupSex.csv

Source file: `tblLookupSex.csv`
Rows: 2

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-07 |  |
| strDescription | VARCHAR | 0.0% | Female |  |
| strcode | VARCHAR | 0.0% | F |  |
| idnSex | INTEGER | 0.0% | 2 |  |

## lu_special_issue

Loaded from tblLookupSpecialIssue.csv

Source file: `tblLookupSpecialIssue.csv`
Rows: 43

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 0.0% | 2021-09-21 |  |
| datCreatedOn | DATE | 0.0% | 2021-09-21 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | AG CERTIFICATION/REMAND |  |
| strCode | VARCHAR | 0.0% | AGC |  |
| idnSpecIssue | INTEGER | 0.0% | 42 |  |

## lu_state

U.S. state codes

Source file: `tblLookupState.csv`
Rows: 59

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 0.0% | true |  |
| datModifiedOn | DATE | 98.3% | 2009-11-06 |  |
| datCreatedOn | DATE | 0.0% | 2002-12-17 |  |
| state_name | VARCHAR | 0.0% | <All> |  |
| state_code | VARCHAR | 0.0% | <A> |  |
| idnState | INTEGER | 0.0% | 58 |  |

## lu_teams

Loaded from tblTeams.csv

Source file: `tblTeams.csv`
Rows: 37

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 16.2% | 2002-05-07 |  |
| datCreatedOn | DATE | 0.0% | 2011-11-11 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Team A |  |
| strCode | VARCHAR | 0.0% | A |  |
| idnTeam | INTEGER | 0.0% | 5 |  |

## lu_torture_conv_id

Loaded from tblTortureConvID.csv

Source file: `tblTortureConvID.csv`
Rows: 8

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| blnActive | BOOLEAN | 100.0% |  |  |
| datModifiedOn | DATE | 100.0% |  |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| strDescription | VARCHAR | 0.0% | CREDIBLE FEAR |  |
| strcode | VARCHAR | 0.0% | 1A |  |
| idnTorConv | INTEGER | 0.0% | 1 |  |

## lu_transcribers

Loaded from tblLookupTranscribers.csv

Source file: `tblLookupTranscribers.csv`
Rows: 14

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| curCostDecisionPriority | VARCHAR | 0.0% | 1.9700 |  |
| curCostTransPriority | VARCHAR | 0.0% | 1.9700 |  |
| curCostPerPageRetype | VARCHAR | 14.3% | .2100 |  |
| curCostPerPageReprint | VARCHAR | 0.0% | .0000 |  |
| curCostPerPageDecision | VARCHAR | 0.0% | 1.7100 |  |
| datModifiedOn | DATE | 71.4% | 1997-03-03 |  |
| datCreatedOn | DATE | 0.0% | 1997-03-23 |  |
| blnActive | BOOLEAN | 0.0% | false |  |
| curCostPerPageTrans | VARCHAR | 0.0% | 1.7100 |  |
| intTAT | VARCHAR | 0.0% | 0 |  |
| idnTranscribers | INTEGER | 0.0% | 1 |  |

## lu_vote

Loaded from tblLookupVote.csv

Source file: `tblLookupVote.csv`
Rows: 7

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datModifiedOn | DATE | 57.1% | 2004-10-25 |  |
| datCreatedOn | DATE | 0.0% | 2003-08-10 |  |
| blnActive | BOOLEAN | 0.0% | true |  |
| strDescription | VARCHAR | 0.0% | Concur |  |
| strCode | VARCHAR | 0.0% | Concur |  |
| idnVote | INTEGER | 0.0% | 1 |  |

## motions

Motions filed in proceedings

Source file: `tbl_Court_Motions.csv`
Rows: 9,178,220

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| column42 | VARCHAR | 100.0% | A.2.a |  |
| column41 | VARCHAR | 99.4% | 2007-03-08 20:01:00.000 |  |
| strDJScenario | VARCHAR | 0.0% | 0 |  |
| BLNDELETED_Remove | BOOLEAN | 100.0% |  |  |
| SCHEDULED_HEAR_LOC_Remove | VARCHAR | 97.8% | WAS |  |
| O_CLOCK_OPTION | VARCHAR | 99.0% | S |  |
| E_28_DATE | DATE | 99.0% | 1988-12-02 |  |
| E_28_RECPTFLAG_Remove | VARCHAR | 100.0% |  |  |
| STRCERTOFSERVICECODE | VARCHAR | 14.0% | C |  |
| STRFILINGMETHOD | VARCHAR | 4.7% | W |  |
| STRFILINGPARTY | VARCHAR | 2.9% | 0000000000 |  |
| DATE_TO_BIA_UPDATE_Remove | DATE | 100.0% |  |  |
| DATE_RECD_FROM_BIA | DATE | 100.0% | 1974-11-14 |  |
| DATE_MAILED_TO_IJ | DATE | 100.0% | 1974-11-14 |  |
| DECISION_RENDERED_Remove | VARCHAR | 100.0% | 1974-11-14 00:00:00.000 |  |
| DATE_TO_BIA | DATE | 100.0% | 1974-11-14 |  |
| DATE_APPEAL_DUE | DATE | 94.0% | 2002-02-19 |  |
| JURISDICTION | VARCHAR | 98.1% | IJ |  |
| STAY_GRANT | VARCHAR | 99.8% | G |  |
| RESP_DUE_DATE | DATE | 4.3% | 1986-08-20 |  |
| APPEAL_NOT_FILED | VARCHAR | 99.7% | X |  |
| APPEAL_RSVD | VARCHAR | 99.0% | A |  |
| WU_MSG_Remove | VARCHAR | 99.7% | FF |  |
| DATMOTIONDUE | DATE | 94.7% | 1986-06-17 |  |
| MOTION_RECD_DATE | DATE | 0.5% | 1988-02-18 |  |
| COMP_DATE | DATE | 1.6% | 1989-08-14 |  |
| DEC | VARCHAR | 1.6% | G |  |
| IJ_NAME_Remove | VARCHAR | 97.2% | JOAN V. CHURCHILL |  |
| IJ_CODE | VARCHAR | 3.0% | JVC | Joins to lu_judge.JUDGE_CODE |
| HEARING_LOC_CODE | VARCHAR | 0.0% | WAS | Joins to lu_hearing_location.LOC_CODE |
| BASE_CITY_CODE | VARCHAR | 0.0% | WAS | Joins to lu_base_city.BASE_CITY_CODE |
| REJ_Remove | VARCHAR | 100.0% | A |  |
| INPUT_TIME | VARCHAR | 0.0% | 1507 |  |
| INPUT_DATE | DATE | 0.0% | 1989-02-22 |  |
| UPDATE_TIME | VARCHAR | 0.2% | 0853 |  |
| UPDATE_DATE | DATE | 0.2% | 1989-08-16 |  |
| SUB_GENERATION | VARCHAR | 97.0% | 99 |  |
| GENERATION | VARCHAR | 0.0% | 99 |  |
| REC_TYPE | VARCHAR | 0.0% | SA |  |
| OSC_DATE | DATE | 0.0% | 1968-10-24 |  |
| IDNCASE | INTEGER | 0.0% | 2047365 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 41 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNMOTION | INTEGER | 0.0% | 20 |  |

## pro_bono

Pro bono screening records

Source file: `tblProBono.csv`
Rows: 68,485

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| strCustody | VARCHAR | 5.9% | D |  |
| datModifiedOn | DATE | 82.7% | 2011-11-29 |  |
| datCreatedOn | DATE | 5.8% | 2011-11-22 |  |
| blnProcessed | BOOLEAN | 0.0% | false |  |
| strDCZip | VARCHAR | 96.9% | 773510000 |  |
| strDCState | VARCHAR | 96.9% | TX |  |
| strDCCity | VARCHAR | 96.6% | Livingston |  |
| strDCAddress2 | VARCHAR | 96.6% | 3400 FM 350 South |  |
| strDCAddress1 | VARCHAR | 96.5% | DHS/ICE OCC -POK |  |
| ScreenerIdn | VARCHAR | 95.4% | 0 |  |
| strIntrprLang | VARCHAR | 100.0% | GER |  |
| blnIntrpr | BOOLEAN | 0.0% | false |  |
| idnRepl | INTEGER | 0.0% | 21739 |  |
| datBriefCurrentlyDueAlien | DATE | 0.9% | 2011-12-07 |  |
| idnAppeal | INTEGER | 0.5% | 4963319 |  |
| datmailedtoNGO | DATE | 96.4% | 2011-12-05 |  |
| Inmate_Housing | VARCHAR | 90.8% | RVS |  |
| Date_Of_Entry | DATE | 30.7% | 2011-04-08 |  |
| CaseID | VARCHAR | 78.2% | EM |  |
| DCO_Location | VARCHAR | 91.3% | F |  |
| OptionA | VARCHAR | 0.0% | -1 |  |
| blnSelectedbyScreener | BOOLEAN | 0.0% | false |  |
| blnSelectedbyCoordinator | BOOLEAN | 0.0% | false |  |
| datROPreview | DATE | 95.9% | 2011-12-02 |  |
| strPossibility | VARCHAR | 99.8% | Low Possibility |  |
| strINSStatus | VARCHAR | 96.3% | Lawful Permanent Resident |  |
| strAlienSex | VARCHAR | 50.6% | M |  |
| strAlienRegion | VARCHAR | 96.5% | Livingston, TX |  |
| strA3 | VARCHAR | 100.0% |  |  |
| strA2 | VARCHAR | 100.0% |  |  |
| strA1 | VARCHAR | 100.0% |  |  |
| CRIM_IND | VARCHAR | 0.5% | N |  |
| other_comp | VARCHAR | 99.5% | O |  |
| Dec_Code | VARCHAR | 0.5% | X |  |
| P_EOIR42B_Recd | VARCHAR | 89.6% | X |  |
| P_EOIR42A_Recd | VARCHAR | 95.6% | X |  |
| WD_recd | VARCHAR | 100.0% | X |  |
| VD_recd | VARCHAR | 87.8% | X |  |
| recd_245 | VARCHAR | 98.2% | X |  |
| recd_212C | VARCHAR | 99.7% | X |  |
| Charge_6 | VARCHAR | 99.9% | 237a02C |  |
| Charge_5 | VARCHAR | 99.6% | 212a02Ai  I |  |
| Charge_4 | VARCHAR | 98.7% | 212a02Ai  I |  |
| Charge_3 | VARCHAR | 95.2% | 212a02Ai  II |  |
| Charge_2 | VARCHAR | 79.7% | 237a02Aiii |  |
| Charge_1 | VARCHAR | 1.1% | 237a02Aiii |  |
| blnOARequestedbyAmicus | BOOLEAN | 0.0% | false |  |
| blnOARequestedbyINS | BOOLEAN | 0.0% | false |  |
| blnOARequestedbyAlien | BOOLEAN | 0.0% | false |  |
| strLang | VARCHAR | 0.2% | SP |  |
| strNat | VARCHAR | 0.2% | MEXICO |  |
| strFiledby | VARCHAR | 0.0% | A |  |
| strProceedingIHP | VARCHAR | 92.4% | F |  |
| datTranscriptServedAlien | DATE | 0.0% | 2011-11-16 |  |
| strCorFacility | VARCHAR | 92.5% | F |  |
| WD_DEC | VARCHAR | 100.0% | D |  |
| VD_DEC | VARCHAR | 87.8% | G |  |
| P_EOIR42B_DEC | VARCHAR | 89.6% | D |  |
| P_EOIR42A_DEC | VARCHAR | 95.6% | D |  |
| Other_dec2 | VARCHAR | 99.8% | D |  |
| Other_dec1 | VARCHAR | 35.5% | D |  |
| NBR_OF_CHGS | VARCHAR | 1.0% | 02 |  |
| DEC_245 | VARCHAR | 98.2% | D |  |
| DEC_212C | VARCHAR | 99.7% | D |  |
| Case_type | VARCHAR | 0.0% | RMV |  |

## proceedings

Proceedings: hearings, decisions, judges, charges

Source file: `B_TblProceeding.csv`
Rows: 16,899,722

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| DATE_RELEASED | DATE | 86.6% | 2026-09-13 |  |
| DATE_DETAINED | DATE | 75.9% | 2026-09-14 |  |
| AGGRAVATE_FELON | BOOLEAN | 25.4% | false |  |
| IHP | VARCHAR | 98.4% | S |  |
| CRIM_IND | VARCHAR | 2.5% | N |  |
| CORRECTIONAL_FAC | VARCHAR | 97.7% | S |  |
| SCHEDULED_HEAR_LOC | VARCHAR | 74.6% | SNA |  |
| LANG | VARCHAR | 0.1% | SP | Joins to lu_language.LANG_CODE |
| NAT | VARCHAR | 0.3% | MX | Joins to lu_nationality.NAT_CODE |
| CASE_TYPE | VARCHAR | 0.0% | RMV | Joins to lu_case_type.CASE_TYPE_CODE |
| CUSTODY | VARCHAR | 0.0% | D | Joins to lu_custody_status.CUSTODY_CODE |
| TRANSFER_STATUS | VARCHAR | 67.2% | C |  |
| DATE_APPEAL_DUE_STATUS | DATE | 30.0% | 2026-10-21 |  |
| TRANSFER_TO | VARCHAR | 81.0% | EDN |  |
| VENUE_CHG_GRANTED | DATE | 81.0% | 2026-09-21 |  |
| ABSENTIA | VARCHAR | 21.0% | N |  |
| COMP_DATE | DATE | 18.0% | 2026-09-21 |  |
| APPEAL_NOT_FILED | VARCHAR | 96.8% | X |  |
| APPEAL_RSVD | VARCHAR | 89.2% | B |  |
| OTHER_COMP | VARCHAR | 73.7% | T |  |
| DEPORTED_2 | VARCHAR | 99.1% | HO |  |
| DEPORTED_1 | VARCHAR | 62.6% | ES |  |
| DEC_CODE | VARCHAR | 44.3% | V | Joins to lu_court_decision.COURT_DEC_CODE |
| DEC_TYPE | VARCHAR | 32.7% | W |  |
| HEARING_TIME | VARCHAR | 3.2% | 0830 |  |
| HEARING_DATE | DATE | 3.3% | 2026-09-30 |  |
| TRANS_NBR | VARCHAR | 62.6% | 02 |  |
| PREV_IJ_CODE | VARCHAR | 81.2% | NIC |  |
| PREV_HEARING_BASE | VARCHAR | 81.0% | OTM |  |
| PREV_HEARING_LOC | VARCHAR | 81.0% | OTM |  |
| TRANS_IN_DATE | DATE | 81.1% | 2026-09-14 |  |
| IJ_CODE | VARCHAR | 0.9% | WJW | Joins to lu_judge.JUDGE_CODE |
| HEARING_LOC_CODE | VARCHAR | 0.0% | IDP | Joins to lu_hearing_location.LOC_CODE |
| BASE_CITY_CODE | VARCHAR | 0.0% | INP | Joins to lu_base_city.BASE_CITY_CODE |
| INPUT_DATE | DATE | 2.8% | 2026-09-14 |  |
| OSC_DATE | DATE | 0.6% | 2026-09-14 |  |
| IDNCASE | INTEGER | 0.0% | 15762247 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 17539552 | Proceeding ID, joins proceedings to charges, schedule, motions |

## representatives

Attorney representation records

Source file: `tbl_RepsAssigned.csv`
Rows: 27,105,444

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| BLNPRIMEATTY | BOOLEAN | 0.0% | true |  |
| E_28_DATE | DATE | 51.6% | 2004-04-06 |  |
| E_27_DATE | DATE | 95.9% | 2004-02-04 |  |
| INS_TA_DATE_ASSIGNED | VARCHAR | 44.0% | 2009-03-10 00:00:00.000 |  |
| BASE_CITY_CODE | VARCHAR | 35.2% | WAS | Joins to lu_base_city.BASE_CITY_CODE |
| PARENT_IDN | VARCHAR | 0.0% | 2047751 |  |
| PARENT_TABLE | VARCHAR | 0.0% | A_Tblcase |  |
| STRATTYTYPE | VARCHAR | 0.0% | ALIEN |  |
| STRATTYLEVEL | VARCHAR | 0.0% | COURT |  |
| IDNCASE | INTEGER | 0.0% | 2047751 | Primary case ID, joins across all core tables |
| IDNREPSASSIGNED | INTEGER | 0.0% | 5945062 |  |

## schedule

Hearing schedule entries with calendar type and adjournments

Source file: `tbl_schedule.csv`
Rows: 48,492,125

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| EOIRAttorneyID | VARCHAR | 59.2% | FBE5787E-68C3-4F0A-90C3-96668DB4D289 |  |
| BLNCLOCKOVERRIDE | BOOLEAN | 0.0% | false |  |
| NOTICE_CODE | VARCHAR | 42.1% | 7P |  |
| SCHEDULE_TYPE | VARCHAR | 0.0% | MM |  |
| LNGSESSNID | VARCHAR | 19.5% | 19308066 |  |
| ADJ_ELAP_DAYS | VARCHAR | 47.0% | 0311 |  |
| ADJ_MSG | VARCHAR | 85.9% | V6 |  |
| ADJ_MEDIUM | VARCHAR | 11.9% | A |  |
| ADJ_RSN | VARCHAR | 13.3% | 32 | Joins to lu_adjournment.ADJ_RSN_CODE |
| ADJ_TIME_STOP | VARCHAR | 10.0% | 1330 |  |
| ADJ_TIME_START | VARCHAR | 0.0% | 1300 |  |
| ADJ_DATE | DATE | 0.0% | 2025-08-14 |  |
| CAL_TYPE | VARCHAR | 0.0% | M | Joins to lu_cal_type.CAL_TYPE_CODE |
| ASSIGNMENT_PATH | VARCHAR | 85.3% | W |  |
| UPDATE_TIME | VARCHAR | 0.0% | 0135 |  |
| UPDATE_DATE | DATE | 0.0% | 2025-08-14 |  |
| INPUT_TIME | VARCHAR | 0.0% | 1241 |  |
| INPUT_DATE | DATE | 0.0% | 2023-12-19 |  |
| INTERPRETER_CODE | VARCHAR | 100.0% | KSS |  |
| IJ_CODE | VARCHAR | 0.0% | LTF | Joins to lu_judge.JUDGE_CODE |
| BASE_CITY_CODE | VARCHAR | 0.0% | PHI | Joins to lu_base_city.BASE_CITY_CODE |
| HEARING_LOC_CODE | VARCHAR | 0.0% | PHI | Joins to lu_hearing_location.LOC_CODE |
| LANG | VARCHAR | 3.5% | CRE | Joins to lu_language.LANG_CODE |
| REC_TYPE | VARCHAR | 0.0% | X |  |
| SUB_GENERATION | VARCHAR | 95.4% | 99 |  |
| GENERATION | VARCHAR | 0.0% | 99 |  |
| OSC_DATE | DATE | 0.0% | 2023-12-19 |  |
| IDNCASE | INTEGER | 0.0% | 13250605 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 13821713 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNSCHEDULE | INTEGER | 0.0% | 47975332 |  |

## three_mbr_referrals

Loaded from tblThreeMbrReferrals.csv

Source file: `tblThreeMbrReferrals.csv`
Rows: 85,059

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| datRemovedFromReferral | DATE | 86.7% | 2002-12-20 |  |
| datReferredTo3Member | DATE | 0.0% | 2002-10-02 |  |
| lngAppealID | VARCHAR | 0.0% | 4525403 |  |
| idn3MemberReferral | INTEGER | 0.0% | 1 |  |

## v_case_summary

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| IDNCASE | INTEGER | 0.0% | 14258892 | Primary case ID, joins across all core tables |
| NAT | VARCHAR | 1.0% | BL | Joins to lu_nationality.NAT_CODE |
| country | VARCHAR | 1.0% | BOLIVIA |  |
| LANG | VARCHAR | 1.2% | SP | Joins to lu_language.LANG_CODE |
| Sex | VARCHAR | 44.9% | M |  |
| CUSTODY | VARCHAR | 1.0% | D | Joins to lu_custody_status.CUSTODY_CODE |
| CASE_TYPE | VARCHAR | 0.0% | CFR | Joins to lu_case_type.CASE_TYPE_CODE |
| LPR | BOOLEAN | 91.7% | false |  |
| DATE_OF_ENTRY | DATE | 38.7% | 2019-08-18 |  |
| C_BIRTHDATE | DATE | 100.0% | 2024-05-31 |  |
| DATE_DETAINED | DATE | 81.1% | 2024-06-24 |  |
| DATE_RELEASED | DATE | 92.4% | 2005-05-11 |  |
| DETENTION_FACILITY_TYPE | VARCHAR | 100.0% | M |  |
| first_hearing_date | DATE | 2.5% | 2024-06-25 |  |
| OSC_DATE | DATE | 2.5% | 2024-06-24 |  |
| days_osc_to_hearing | BIGINT | 2.5% | 1 |  |
| days_detained_to_hearing | BIGINT | 82.6% | 1 |  |
| BASE_CITY_CODE | VARCHAR | 2.5% | SDC | Joins to lu_base_city.BASE_CITY_CODE |

## v_first_hearing

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| IDNCASE | INTEGER | 0.0% | 15762247 | Primary case ID, joins across all core tables |
| IDNPROCEEDING | INTEGER | 0.0% | 3199700 | Proceeding ID, joins proceedings to charges, schedule, motions |
| BASE_CITY_CODE | VARCHAR | 0.0% | OMA | Joins to lu_base_city.BASE_CITY_CODE |
| CUSTODY | VARCHAR | 0.0% | D | Joins to lu_custody_status.CUSTODY_CODE |
| CASE_TYPE | VARCHAR | 0.0% | RMV | Joins to lu_case_type.CASE_TYPE_CODE |
| OSC_DATE | DATE | 0.0% | 2019-04-17 |  |
| first_hearing_date | DATE | 0.0% | 2026-09-30 |  |
| DATE_DETAINED | DATE | 82.1% | 2006-10-24 |  |
| days_osc_to_hearing | BIGINT | 0.0% | 10 |  |
| days_detained_to_hearing | BIGINT | 82.1% | 58 |  |
| days_detained_to_osc | BIGINT | 82.1% | 0 |  |

## v_proceedings_full

| Column | Type | Nulls | Example | Join |
|--------|------|-------|---------|------|
| IDNPROCEEDING | INTEGER | 0.0% | 17539558 | Proceeding ID, joins proceedings to charges, schedule, motions |
| IDNCASE | INTEGER | 0.0% | 15617588 | Primary case ID, joins across all core tables |
| OSC_DATE | DATE | 0.6% | 2026-06-26 |  |
| INPUT_DATE | DATE | 2.8% | 2026-09-14 |  |
| BASE_CITY_CODE | VARCHAR | 0.0% | JNA | Joins to lu_base_city.BASE_CITY_CODE |
| HEARING_LOC_CODE | VARCHAR | 0.0% | RCC | Joins to lu_hearing_location.LOC_CODE |
| IJ_CODE | VARCHAR | 0.9% | FSM | Joins to lu_judge.JUDGE_CODE |
| TRANS_IN_DATE | DATE | 81.1% | 2026-09-24 |  |
| PREV_HEARING_LOC | VARCHAR | 81.0% | DED |  |
| PREV_HEARING_BASE | VARCHAR | 81.0% | DET |  |
| PREV_IJ_CODE | VARCHAR | 81.2% | MJJ |  |
| TRANS_NBR | VARCHAR | 62.6% | 02 |  |
| HEARING_DATE | DATE | 3.3% | 2026-09-30 |  |
| HEARING_TIME | VARCHAR | 3.2% | 0930 |  |
| DEC_TYPE | VARCHAR | 32.7% | W |  |
| DEC_CODE | VARCHAR | 44.3% | V | Joins to lu_court_decision.COURT_DEC_CODE |
| DEPORTED_1 | VARCHAR | 62.6% | ES |  |
| DEPORTED_2 | VARCHAR | 99.1% | HO |  |
| OTHER_COMP | VARCHAR | 73.7% | T |  |
| APPEAL_RSVD | VARCHAR | 89.2% | B |  |
| APPEAL_NOT_FILED | VARCHAR | 96.8% | X |  |
| COMP_DATE | DATE | 18.0% | 2026-09-30 |  |
| ABSENTIA | VARCHAR | 21.0% | N |  |
| VENUE_CHG_GRANTED | DATE | 81.0% | 2026-09-21 |  |
| TRANSFER_TO | VARCHAR | 81.0% | EDN |  |
| DATE_APPEAL_DUE_STATUS | DATE | 30.0% | 2026-10-30 |  |
| TRANSFER_STATUS | VARCHAR | 67.2% | C |  |
| CUSTODY | VARCHAR | 0.0% | D | Joins to lu_custody_status.CUSTODY_CODE |
| CASE_TYPE | VARCHAR | 0.0% | RMV | Joins to lu_case_type.CASE_TYPE_CODE |
| NAT | VARCHAR | 0.3% | ES | Joins to lu_nationality.NAT_CODE |
| LANG | VARCHAR | 0.1% | SP | Joins to lu_language.LANG_CODE |
| SCHEDULED_HEAR_LOC | VARCHAR | 74.6% | SNA |  |
| CORRECTIONAL_FAC | VARCHAR | 97.7% | S |  |
| CRIM_IND | VARCHAR | 2.5% | N |  |
| IHP | VARCHAR | 98.4% | S |  |
| AGGRAVATE_FELON | BOOLEAN | 25.4% | false |  |
| DATE_DETAINED | DATE | 75.9% | 2026-06-26 |  |
| DATE_RELEASED | DATE | 86.6% | 2019-11-22 |  |
| court_name | VARCHAR | 0.0% | IMMIGRATION COURT |  |
| judge_name | VARCHAR | 1.1% | Mwangi, Francis |  |
| nationality_name | VARCHAR | 0.3% | EL SALVADOR |  |
| country_name | VARCHAR | 0.3% | EL SALVADOR |  |
| decision_description | VARCHAR | 44.3% | Voluntary Departure |  |
