# Task 4 — Data Issues Report

All issues below were found across `source1_naukri_applicants.csv`, `source2_gig_workers.csv`,
and `source3_cbnexus_contacts.csv`. Every fix described here is implemented in `ingestion_pipeline.py`.
A full per-row log of every conversion/decision the pipeline actually made on a given run is
written to `docs/ingestion_log.txt`.

---

## 1. Column-shifted row (gig_workers)

**Issue:** Row 18 has its fields shifted one column to the right — the `email_id` field contains
skills text (`"react, javascript, mysql"`), the person's actual name (`"Isha Chopra"`) sits in the
`rate` column, and the real rate (`"1406/hr"`) sits in `location`.

**Resolution:** Detected via a check on `email_id` — if it doesn't contain `@`, the row is
realigned before any other processing: `email_id`, `worker_name`, `rate`, `location`, `status`,
and `skill_tags` are each shifted back to their correct field. Logged when it fires.

---

## 2. Fully-null row (gig_workers)

**Issue:** Row 10 has every field blank — a stray blank line from the CSV export.

**Resolution:** Detected and skipped, logged explicitly rather than silently dropped so it's
traceable in the ingestion log.

---

## 3. Stray header row embedded as data (cbnexus, and checked across all 3 sources)

**Issue:** Row 14 in cbnexus has every column's value equal to its own column name
(`Name="Name"`, `"Phone Number"="Phone Number"`, etc.) — a duplicated header row that leaked
into the data.

**Resolution:** A general `is_header_row()` check compares every value in a row against its
own column name; if all match, the row is skipped. Applied to all three loaders (not just
cbnexus), even though a manual check confirmed this specific bug only occurs in cbnexus row 14
— naukri and gig_workers came back clean when checked directly against the same test.

---

## 4. Phone number format inconsistency (naukri, cbnexus)

**Issue:** At least 4 formats in use: `9000000254`, `919000000254`, `+91-9000000131`,
`09000000287`.

**Resolution:** Strip all non-digit characters, keep the last 10 digits, store as `TEXT` (not
a numeric type — casting to int would silently drop leading zeros and misrepresent
country-code-prefixed numbers).

---

## 5. City name inconsistency (all 3 sources)

**Issue:** Casing and spelling variants: `Bangalore`/`Bengaluru`, `Delhi`/`New Delhi`/
`Delhi NCR`, `Gurgaon`/`Gurugram`, plus casing (`PUNE`/`pune`) and trailing whitespace
(`"Noida "`).

**Resolution:** Explicit canonical mapping applied during normalization:
`Bengaluru`, `Delhi`, `Gurgaon`, `Noida`, `Pune`. Unmapped cities fall back to
`.strip().title()`.

---

## 6. `status` casing inconsistency (gig_workers)

**Issue:** Mixed casing across `Active`/`active`/`ACTIVE`/`Inactive`/`paused`.

**Resolution:** Lowercased and stripped only — states are kept distinct (not collapsed to a
binary active/inactive), since `paused` is a meaningfully different state worth preserving.

---

## 7. `Verified` inconsistency (cbnexus)

**Issue:** 6 distinct raw values for what's conceptually a yes/no field: `Y`, `Yes`, `yes`,
`Verified`, `N`, `No`.

**Resolution:** Collapsed to a strict boolean — `Y`/`Yes`/`Verified` → `true`, `N`/`No` →
`false`. Stored in the `verified BOOLEAN` column.

---

## 8. `Projects Completed` typed as string (cbnexus)

**Issue:** Column is text, not numeric, and includes at least one non-numeric value.

**Resolution:** Safe numeric cast (`to_int_safe`); any row that fails to cast is logged rather
than crashing the pipeline or silently defaulting to 0.

---

## 9. `Applied Date` — multiple formats (naukri)

**Issue:** At least 4 distinct date formats present: `24-07-2026`, `2026-08-08`, `7 Jul 2026`,
`08/13/2026`.

**Resolution:** Parsed with `dateutil.parser.parse(dayfirst=True)`, wrapped in a try/except.
Unparseable dates are logged and stored as `NULL` rather than guessed.

---

## 10. `Current CTC` — mixed units (naukri)

**Issue:** Some rows report CTC already in lakhs (e.g. `7.8`, `6.6`), others in raw rupees
(e.g. `456780`). Naively dividing every value by 100,000 destroyed the already-in-lakhs rows
(`7.8` → `0.0`).

**Resolution:** Values under a 60,000 threshold are treated as already-in-lakhs and kept as-is
(no legitimate annual salary is literally ₹60,000, so a value that small must already be in
lakhs). Values at/above the threshold are divided by 100,000 and **truncated** — not rounded —
to 2 decimals (`456780` → `4.56`). This threshold is a judgment call, not derived from the
data itself, and is called out as an assumption rather than a certainty.

---

## 11. `rate` — mixed units (gig_workers)

**Issue:** Same column holds two different formats: `"1415/hr"` and `"55k/month"`.

**Resolution:** `/hr` values kept as-is. `/month` values (with `k` = thousands) converted to
hourly using an assumed **160 hrs/month** (~20 working days × 8 hrs/day) — again a documented
assumption, since the source data gives no actual hours-worked figure. Final stored value is
numeric only, with no unit suffix.

---

## 12. Name inconsistency and abbreviation (naukri)

**Issue:** Same person appears as `"R. Verma"` and `"Rohit Verma"` in two separate rows within
the same file, tied together only by a shared email address.

**Resolution:** Names are uppercased on ingestion. Abbreviation is resolved **at merge time**,
not at ingestion — when two records match (e.g. on shared email), whichever name is not an
initial-plus-surname pattern (or is longer) is kept as the canonical name going forward. Every
resolution is logged (`"name resolved on merge: 'R. VERMA' -> 'ROHIT VERMA'"`).

---

## 13. Cross-source duplicates with no overlapping identifier

**Issue:** Some candidates appear across sources with email populated on one side and phone on
the other (never both), so a pure email-or-phone match finds nothing to compare — e.g. Karan
Chopra appears once with a phone number and no email, and once with an email and no phone.

**Resolution:** Matching uses a composite score rather than a strict waterfall: exact email
(+50), exact phone (+35), exact full-name match (+30, since names are consistently uppercased),
fuzzy surname/first-name (+10/+5) as a fallback when the name isn't an exact match, and city
match (+20). A score ≥ 50 auto-merges, 30–49 is inserted as new but flagged `needs_review`, and
anything lower stays separate. Exact name + city alone reaches the 50-point merge threshold,
which is what correctly merges the Karan Chopra / Arjun Mehta / Manish Bhatia cases where
email and phone never overlap directly.

**Known risk:** name+city matching with no email/phone evidence is inherently probabilistic —
two genuinely different people sharing a common name and city with no other identifying data
would still merge under this logic. Accepted given the dataset's size, but not something to
gloss over if pushed on it.

---

## 14. Conflict guard against over-merging

**Issue:** Once name+city alone could trigger an auto-merge, it became possible to wrongly
merge two *different* people who happen to share a name and city but have genuinely different
emails or phone numbers.

**Resolution:** A `has_conflict()` check runs before scoring: if both records have a non-null
email (or phone) and the values differ, the merge is blocked outright regardless of how high
the name/city score is. Verified this blocks the case of two same-name, same-city candidates
with conflicting emails, while still correctly merging the cases where email/phone are simply
absent (not conflicting) on one side.

---

## Summary of judgment calls (not derived from the data itself)

- **60,000 threshold** for detecting already-in-lakhs vs raw-rupee CTC values.
- **160 hrs/month** assumption for converting monthly rates to hourly.
- **City canonical mapping** — the specific spelling chosen for each city (`Bengaluru` over
  `Bangalore`, etc.) is a pick, not a fact.
- **Merge threshold of 50 / review threshold of 30** — tunable, chosen based on manually
  checking a handful of known duplicate/non-duplicate pairs in this dataset.