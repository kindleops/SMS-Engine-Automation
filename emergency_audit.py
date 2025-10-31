#!/usr/bin/env python3
"""
Emergency SMS System Audit - Comprehensive Analysis
Analyzes duplicates, DNC violations, and system status
"""

import sys
from typing import Dict, List, Tuple, Any
from collections import defaultdict, Counter
import traceback

def audit_system() -> Dict[str, Any]:
    """Complete system audit for emergency response"""
    print("🚨 EMERGENCY SMS SYSTEM AUDIT 🚨")
    print("=" * 50)
    
    results = {
        "emergency_status": {},
        "duplicates": {},
        "dnc_status": {},
        "system_health": {},
        "recommendations": []
    }
    
    # 1. Emergency Stop Status
    print("\n1️⃣ EMERGENCY STOP STATUS")
    try:
        from sms.textgrid_sender import ACCOUNT_SID, AUTH_TOKEN
        emergency_active = not (ACCOUNT_SID and AUTH_TOKEN)
        results["emergency_status"] = {
            "active": emergency_active,
            "textgrid_disabled": emergency_active,
            "reason": "logging_issues" if emergency_active else "normal"
        }
        print(f"   🔒 Emergency Stop: {'ACTIVE' if emergency_active else 'INACTIVE'}")
        if emergency_active:
            print("   ✅ System is safely stopped")
        else:
            print("   ⚠️ System can send messages!")
    except Exception as e:
        print(f"   ❌ Error checking emergency status: {e}")
        results["emergency_status"] = {"error": str(e)}
    
    # 2. Duplicate Analysis
    print("\n2️⃣ DUPLICATE ANALYSIS")
    try:
        from sms.config import drip_queue
        drip_tbl = drip_queue()
        if drip_tbl:
            records = drip_tbl.all()
            phone_records = defaultdict(list)
            
            for r in records:
                fields = r.get("fields", {})
                phone = fields.get("Seller Phone Number", "")
                status = fields.get("Status", "")
                if phone:
                    phone_records[phone].append({
                        "id": r.get("id"),
                        "status": status,
                        "campaign": fields.get("Campaign", [""])[0] if isinstance(fields.get("Campaign"), list) else "",
                        "template": fields.get("Template", [""])[0] if isinstance(fields.get("Template"), list) else ""
                    })
            
            # Find duplicates
            duplicates = {phone: records for phone, records in phone_records.items() if len(records) > 1}
            status_counts = Counter()
            duplicate_records = 0
            
            for phone, phone_recs in duplicates.items():
                for rec in phone_recs:
                    status_counts[rec["status"]] += 1
                    duplicate_records += 1
            
            results["duplicates"] = {
                "unique_phones_with_duplicates": len(duplicates),
                "total_duplicate_records": duplicate_records,
                "status_breakdown": dict(status_counts),
                "sample_violations": list(duplicates.keys())[:5]
            }
            
            print(f"   📊 Phones with duplicates: {len(duplicates)}")
            print(f"   📊 Total duplicate records: {duplicate_records}")
            print(f"   📊 Status breakdown: {dict(status_counts)}")
            
            # Check if duplicates already sent
            sent_duplicates = sum(1 for recs in duplicates.values() for rec in recs if rec["status"] in ["Sent", "✅"])
            if sent_duplicates > 0:
                print(f"   🚨 CRITICAL: {sent_duplicates} duplicates already SENT!")
                results["recommendations"].append("URGENT: Investigate duplicate messages already sent to customers")
        else:
            print("   ❌ Could not access drip queue")
            results["duplicates"] = {"error": "table_unavailable"}
    except Exception as e:
        print(f"   ❌ Error analyzing duplicates: {e}")
        results["duplicates"] = {"error": str(e)}
    
    # 3. DNC/Opt-Out Analysis
    print("\n3️⃣ DNC/OPT-OUT ANALYSIS")
    try:
        from sms.config import table_control, drip_queue
        
        # Get opt-out numbers
        optouts_tbl = table_control('Opt-Outs')
        optout_numbers = set()
        if optouts_tbl:
            records = optouts_tbl.all()
            for r in records:
                fields = r.get('fields', {})
                phone = fields.get('Phone', fields.get('Name', ''))
                if phone:
                    optout_numbers.add(phone.strip())
        
        # Check for violations in drip queue
        drip_tbl = drip_queue()
        violations = []
        if drip_tbl:
            records = drip_tbl.all()
            for r in records:
                fields = r.get('fields', {})
                phone = fields.get('Seller Phone Number', '')
                status = fields.get('Status', '')
                if phone in optout_numbers:
                    violations.append({
                        "phone": phone,
                        "status": status,
                        "record_id": r.get('id')
                    })
        
        results["dnc_status"] = {
            "total_optouts": len(optout_numbers),
            "dnc_violations": len(violations),
            "violation_details": violations
        }
        
        print(f"   📊 Total opt-out numbers: {len(optout_numbers)}")
        print(f"   🚨 DNC violations in queue: {len(violations)}")
        
        if violations:
            print("   ❌ VIOLATION DETAILS:")
            for v in violations:
                print(f"      {v['phone']} ({v['status']}) - {v['record_id']}")
            results["recommendations"].append("URGENT: Remove all opted-out numbers from drip queue immediately")
        else:
            print("   ✅ No DNC violations found in current queue")
            
    except Exception as e:
        print(f"   ❌ Error analyzing DNC status: {e}")
        results["dnc_status"] = {"error": str(e)}
    
    # 4. System Health Check
    print("\n4️⃣ SYSTEM HEALTH CHECK")
    try:
        from sms.tables import ping_all
        health = ping_all(verbose=False)
        results["system_health"] = health
        
        healthy_tables = sum(1 for status in health.values() if status)
        total_tables = len(health)
        
        print(f"   📊 Healthy tables: {healthy_tables}/{total_tables}")
        for table, status in health.items():
            icon = "✅" if status else "❌"
            print(f"      {icon} {table}")
            
        if healthy_tables < total_tables:
            results["recommendations"].append("System health degraded - some tables unreachable")
            
    except Exception as e:
        print(f"   ❌ Error checking system health: {e}")
        results["system_health"] = {"error": str(e)}
    
    # 5. Final Recommendations
    print("\n5️⃣ EMERGENCY RECOMMENDATIONS")
    
    # Add critical recommendations based on findings
    if results.get("emergency_status", {}).get("active"):
        print("   ✅ Emergency stop is active - system is safe")
    else:
        results["recommendations"].insert(0, "CRITICAL: Enable emergency stop immediately")
    
    if results.get("duplicates", {}).get("total_duplicate_records", 0) > 0:
        results["recommendations"].append("Run deduplication script to remove duplicate records")
    
    if not results["recommendations"]:
        results["recommendations"].append("System appears stable - maintain emergency stop until logging issues resolved")
    
    for i, rec in enumerate(results["recommendations"], 1):
        print(f"   {i}. {rec}")
    
    print("\n" + "=" * 50)
    print("🛡️ AUDIT COMPLETE")
    return results

if __name__ == "__main__":
    try:
        results = audit_system()
        
        # Exit with error code if critical issues found
        critical_issues = (
            not results.get("emergency_status", {}).get("active", True) or
            results.get("dnc_status", {}).get("dnc_violations", 0) > 0 or
            results.get("duplicates", {}).get("total_duplicate_records", 0) > 1000
        )
        
        if critical_issues:
            print("\n🚨 CRITICAL ISSUES DETECTED - SYSTEM UNSAFE")
            sys.exit(1)
        else:
            print("\n✅ SYSTEM STATUS: EMERGENCY CONTAINED")
            sys.exit(0)
            
    except Exception as e:
        print(f"\n❌ AUDIT FAILED: {e}")
        traceback.print_exc()
        sys.exit(2)