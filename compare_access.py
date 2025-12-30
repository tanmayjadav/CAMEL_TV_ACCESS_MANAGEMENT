"""
Match TV users from CSV with WordPress transactions from JSON.
Add email and expiry based on matching tradingview_username.
"""
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def parse_expiry(expiry_str: str) -> Optional[str]:
    """Parse expiry date string and return ISO format date string"""
    if not expiry_str:
        return None
    try:
        # Handle various date formats
        if "T" in expiry_str or " " in expiry_str:
            dt = datetime.fromisoformat(expiry_str.replace("Z", "+00:00"))
            return dt.date().isoformat()
        dt = datetime.strptime(expiry_str, "%Y-%m-%d")
        return dt.date().isoformat()
    except:
        return None


def load_wordpress_transactions(json_path: Path) -> Dict[str, Dict]:
    """Load WordPress transactions from JSON and create lookup by tradingview_username and user_login
    
    Returns:
        Dict mapping lowercase username to transaction info with email, expiry, and match source info
    """
    print(f"📥 Loading WordPress transactions from: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        transactions = json.load(f)
    
    print(f"   Found {len(transactions)} transactions")
    
    # Create lookup dictionary by username (lowercase)
    # Check both tradingview_username from user_meta and user_login
    # Keep the latest transaction per user (by expiry date)
    # Also track which field(s) were used and if there's a mismatch
    lookup: Dict[str, Dict] = {}
    
    for txn in transactions:
        # Get email and expiry
        email = txn.get("user_email", "").strip()
        expires_at = txn.get("expires_at", "").strip()
        
        # Parse expiry to date string
        expiry_date = parse_expiry(expires_at) if expires_at else None
        
        # Get possible usernames to match against
        user_meta = txn.get("user_meta", {})
        tv_username_meta = user_meta.get("tradingview_username", "").strip() if user_meta else ""
        user_login = txn.get("user_login", "").strip()
        
        # Check if there's a mismatch (user_login exists but differs from tradingview_username)
        has_mismatch = False
        if user_login and tv_username_meta and user_login.lower() != tv_username_meta.lower():
            has_mismatch = True
        
        # Transaction data to store
        txn_data = {
            'email': email,
            'expiry': expiry_date,
            'has_mismatch': has_mismatch,
            'tv_username_meta': tv_username_meta.lower() if tv_username_meta else None,
            'user_login': user_login.lower() if user_login else None,
        }
        
        # Add to lookup by tradingview_username (if exists)
        if tv_username_meta:
            username_lower = tv_username_meta.lower()
            existing = lookup.get(username_lower)
            
            if not existing:
                lookup[username_lower] = txn_data.copy()
                lookup[username_lower]['matched_via'] = 'tradingview_username'
            else:
                # Update if this transaction has a later expiry
                if expiry_date and existing['expiry']:
                    try:
                        existing_date = datetime.fromisoformat(existing['expiry']).date()
                        new_date = datetime.fromisoformat(expiry_date).date()
                        if new_date > existing_date:
                            lookup[username_lower] = txn_data.copy()
                            lookup[username_lower]['matched_via'] = 'tradingview_username'
                    except:
                        pass
                elif expiry_date and not existing['expiry']:
                    lookup[username_lower] = txn_data.copy()
                    lookup[username_lower]['matched_via'] = 'tradingview_username'
                elif not existing['email'] and email:
                    lookup[username_lower] = txn_data.copy()
                    lookup[username_lower]['matched_via'] = 'tradingview_username'
        
        # Add to lookup by user_login (if exists and different from tradingview_username)
        if user_login:
            username_lower = user_login.lower()
            # Only add if different from tradingview_username (to avoid duplicates)
            if not tv_username_meta or username_lower != tv_username_meta.lower():
                existing = lookup.get(username_lower)
                
                if not existing:
                    lookup[username_lower] = txn_data.copy()
                    lookup[username_lower]['matched_via'] = 'user_login'
                else:
                    # Update if this transaction has a later expiry
                    if expiry_date and existing['expiry']:
                        try:
                            existing_date = datetime.fromisoformat(existing['expiry']).date()
                            new_date = datetime.fromisoformat(expiry_date).date()
                            if new_date > existing_date:
                                lookup[username_lower] = txn_data.copy()
                                lookup[username_lower]['matched_via'] = 'user_login'
                        except:
                            pass
                    elif expiry_date and not existing['expiry']:
                        lookup[username_lower] = txn_data.copy()
                        lookup[username_lower]['matched_via'] = 'user_login'
                    elif not existing['email'] and email:
                        lookup[username_lower] = txn_data.copy()
                        lookup[username_lower]['matched_via'] = 'user_login'
    
    print(f"   Created lookup for {len(lookup)} unique usernames")
    return lookup


def main():
    """Main function to match TV users with WordPress transactions"""
    print("=" * 80)
    print("TV USER EMAIL & EXPIRY MATCHING TOOL")
    print("=" * 80)
    print()
    
    # Find latest WordPress transactions JSON file
    output_dir = Path("access_comparison_results")
    json_files = list(output_dir.glob("wordpress_transactions_*.json"))
    
    if not json_files:
        print(f"❌ No WordPress transactions JSON file found in {output_dir}")
        return
    
    # Get the latest JSON file
    latest_json = max(json_files, key=lambda p: p.stat().st_mtime)
    print(f"📄 Using JSON file: {latest_json.name}")
    print()
    
    # Load WordPress transactions lookup
    wp_lookup = load_wordpress_transactions(latest_json)
    print()
    
    # Load TV users from CSV
    csv_path = Path("tv_users_full.csv")
    
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        return
    
    print(f"📥 Processing TV users from CSV: {csv_path}...")
    
    results: List[Dict] = []
    matched_count = 0
    unmatched_count = 0
    tv_name_changed_count = 0
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tv_username = row.get("username", "").strip()
            if not tv_username:
                continue
            
            username_lower = tv_username.lower()
            
            # Check if username exists in WordPress transactions
            wp_info = wp_lookup.get(username_lower)
            
            if wp_info:
                # Match found - add email and expiry from WordPress
                matched_via = wp_info.get('matched_via', '')
                
                # Check if matched via user_login (meaning TV name changed)
                tv_name_changed = ''
                if matched_via == 'user_login':
                    tv_name_changed = 'TV_name_changed'
                    tv_name_changed_count += 1
                
                results.append({
                    'tv_username': tv_username,
                    'email': wp_info.get('email', ''),
                    'expiry': wp_info.get('expiry', ''),
                    'status': tv_name_changed,
                })
                matched_count += 1
            else:
                # No match found - add with empty email and expiry
                results.append({
                    'tv_username': tv_username,
                    'email': '',
                    'expiry': '',
                    'status': '',
                })
                unmatched_count += 1
    
    # Save results to CSV
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = output_dir / f"tv_users_with_email_expiry_{timestamp}.csv"
    
    fieldnames = ['tv_username', 'email', 'expiry', 'status']
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    # Print summary
    print()
    print("=" * 80)
    print("MATCHING SUMMARY")
    print("=" * 80)
    print(f"✅ Matched (found email & expiry): {matched_count}")
    print(f"   └─ Matched via tradingview_username: {matched_count - tv_name_changed_count}")
    print(f"   └─ Matched via user_login (TV_name_changed): {tv_name_changed_count}")
    print(f"⚠️  Unmatched (no WordPress transaction): {unmatched_count}")
    print(f"📊 Total TV users processed: {len(results)}")
    print()
    print(f"✅ Results saved to: {output_csv}")
    print("=" * 80)


if __name__ == "__main__":
    main()
