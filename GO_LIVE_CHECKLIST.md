# SMS Autoresponder Go-Live Checklist

## Overview
Comprehensive checklist for deploying the SMS autoresponder to production with confidence.

## Date: October 27, 2025

---

## Pre-Deployment Configuration Checklist

### ✅ 1. Template Configuration
- [ ] **Intent Keys Match**: Verify Airtable template intent keys match your pools:
  - `stage2_interest_prompt` - Stage 1 → Stage 2 progression
  - `stage3_ask_price` - Stage 2 → Stage 3 progression  
  - `stage4_condition_prompt` - Ask/offer → condition inquiry
  - `stage4_condition_ack_prompt` - Price provided → condition follow-up
  - `handoff_ack` - Final acknowledgment before handoff
  - `followup_30d_queue` - 30-day follow-up messages
- [ ] **Fallback Templates**: Confirm FALLBACK_TEMPLATES constant has appropriate defaults
- [ ] **Template Testing**: Test template rendering with real prospect data

### ✅ 2. Quiet Hours Configuration  
- [ ] **QUIET_HOURS_ENFORCED**: Set to `true` to activate quiet hours
- [ ] **QUIET_START_HOUR**: Configure start hour (e.g., `21` for 9 PM)
- [ ] **QUIET_END_HOUR**: Configure end hour (e.g., `9` for 9 AM)
- [ ] **QUIET_TZ**: Set timezone (e.g., `America/Chicago`)
- [ ] **Test Quiet Window**: Verify `_quiet_window()` logic with your hours

### ✅ 3. Drip Queue Setup
- [ ] **Drip Table Exists**: Confirm drip queue table is created in Airtable
- [ ] **Status Field**: Table has `Status` field for QUEUED messages
- [ ] **Next Send Date**: Table has scheduling field for delayed delivery
- [ ] **Processor Configured**: Separate process consumes `Status=QUEUED` messages
- [ ] **Failure Handling**: Test behavior when drip enqueue fails (should log "Throttled")

### ✅ 4. Database Views & Filters
- [ ] **"Unprocessed Inbounds" View**: Shows only:
  - `Direction` = IN or INBOUND
  - `Processed By` is empty/null
  - No other filters that could hide valid messages
- [ ] **Lead Creation**: Prospects table accepts new lead records
- [ ] **Phone Lookup**: Verify `_find_record_by_phone()` can locate prospects

### ✅ 5. Webhook Integration
- [ ] **Inbound Message Creation**: Webhooks create Conversation rows with:
  - `Direction` field populated (IN/INBOUND)
  - `Message` field with message body
  - `TextGrid Phone Number` field with from number
  - `Seller Phone Number` field with to number
- [ ] **Real-time Testing**: Send test SMS and verify conversation record creation

---

## Quick Smoke Tests (5 minutes)

### 🧪 Test Scenarios

**Test 1: Stage 1 → Stage 2 Progression**
- [ ] Send "Yes" to Stage 1 conversation
- [ ] ✅ Expected: Auto Stage 2 interest prompt (fallback if template missing)
- [ ] ✅ Expected: Prospect marked as phone verified

**Test 2: Not Interested Path**  
- [ ] Send "Not interested" at Stage 1 or 2
- [ ] ✅ Expected: No immediate reply
- [ ] ✅ Expected: Drip queue entry created with +30 day schedule
- [ ] ✅ Expected: Phone marked verified in prospect record

**Test 3: Price Detection**
- [ ] Send "$250k" or "250000" at Stage 3
- [ ] ✅ Expected: Auto Stage 4 condition prompt
- [ ] ✅ Expected: Price captured in Prospect `Asking Price` field

**Test 4: Opt-out Handling**
- [ ] Send "STOP" from any stage
- [ ] ✅ Expected: Stage changes to OPT_OUT
- [ ] ✅ Expected: Status becomes "OPT OUT"  
- [ ] ✅ Expected: Prospect `Opt Out?` = True

**Test 5: Quiet Hours (with working drip)**
- [ ] Send message during configured quiet hours
- [ ] ✅ Expected: Message enqueued for later delivery
- [ ] ✅ Expected: Status logged as "QUEUED"

**Test 6: Quiet Hours (with broken drip)**
- [ ] Temporarily disable drip queue
- [ ] Send message during quiet hours
- [ ] ✅ Expected: No message sent
- [ ] ✅ Expected: Single "Throttled" log entry (NOT "QUEUED")

---

## Advanced Validation

### 🔍 Agent Collision Prevention
- [ ] **Human Agent Reply**: Have agent reply to conversation
- [ ] **30-Minute Window**: Verify autoresponder doesn't interrupt for 30 minutes
- [ ] **Any Agent Type**: Test with different agent names/IDs

### 🔍 Race Condition Prevention  
- [ ] **Multiple Workers**: Run multiple autoresponder instances
- [ ] **Same Record**: Have them process same conversation
- [ ] **Early Claiming**: Verify `Processed By` prevents double-processing

### 🔍 Fallback Template Testing
- [ ] **Empty Template Pool**: Clear all templates for `stage3_ask_price`
- [ ] **Send Valid Message**: Trigger Stage 3 progression
- [ ] **Verify Fallback**: Should use fallback template, not "Thanks for the reply."

### 🔍 Error Handling
- [ ] **Airtable Connectivity**: Test behavior during Airtable outages
- [ ] **Invalid Phone Numbers**: Test malformed phone number handling  
- [ ] **Missing Prospect**: Test conversation without matching prospect record

---

## Performance & Scale Considerations

### 📈 Optimization Opportunities

**Phone Lookups (Optional Improvement)**
- Current: `_find_record_by_phone()` scans all prospect rows (O(n))
- Recommended: Use Airtable `filterByFormula` with last-10-digits matching
- Example: `filterByFormula: "RIGHT({Phone}, 10) = 'last10digits'"`

**Load Testing Targets**
- [ ] **100 concurrent conversations**: System handles without race conditions
- [ ] **1000+ prospect records**: Phone lookup performance acceptable
- [ ] **Peak hour volumes**: Queue processing keeps up with demand

---

## Production Monitoring Setup

### 📊 Key Metrics to Track

**Conversation Flow Health**
- [ ] Stage progression rates (1→2, 2→3, 3→4, 4→handoff)
- [ ] Opt-out rates by stage and time
- [ ] Template fallback usage frequency
- [ ] Average conversation length/duration

**System Health**
- [ ] Processing latency (inbound → response time)
- [ ] Race condition incidents (duplicate processing)
- [ ] Agent collision prevention effectiveness
- [ ] Quiet hours compliance rate

**Error Monitoring**
- [ ] Failed phone lookups
- [ ] Template rendering errors
- [ ] Drip queue failures
- [ ] Airtable API errors

---

## Rollback Plan

### 🚨 Emergency Procedures

**If Issues Detected**
1. **Immediate**: Set `QUIET_HOURS_ENFORCED=true` with 24-hour quiet window
2. **Short-term**: Disable autoresponder processing entirely
3. **Investigation**: Review logs for error patterns
4. **Rollback**: Deploy previous stable version if needed

**Safe Rollback Triggers**
- [ ] Opt-out rate > 10% in any hour
- [ ] Agent collision reports from team
- [ ] Template fallback rate > 50%
- [ ] Processing errors > 5% of volume

---

## Final Go-Live Approval

### ✅ Sign-off Checklist

- [ ] **Configuration Verified**: All settings tested and documented
- [ ] **Smoke Tests Passed**: All 6 core scenarios working
- [ ] **Team Training**: Support team knows how to monitor and troubleshoot  
- [ ] **Rollback Ready**: Emergency procedures documented and tested
- [ ] **Monitoring Active**: Dashboards configured for key metrics

**🚀 Ready for Production Deployment**

---

## Post-Deployment (First 48 Hours)

### 📈 Initial Monitoring

**Hour 1-6: Close Monitoring**
- [ ] Real message processing working correctly
- [ ] No unexpected opt-outs or errors
- [ ] Stage progressions following expected patterns

**Day 1: Pattern Analysis**  
- [ ] Review conversation flow success rates
- [ ] Identify any template optimization opportunities
- [ ] Confirm agent collision prevention working

**Day 2: Performance Validation**
- [ ] System handling production volumes smoothly
- [ ] No race conditions or processing delays
- [ ] Ready to shift to normal monitoring schedule

**Success Criteria**: 
- ✅ <2% error rate
- ✅ >80% stage 1→2 progression on "yes" responses  
- ✅ Zero agent collision incidents
- ✅ All quiet hours respected

🎯 **Deployment Status: READY TO SHIP**