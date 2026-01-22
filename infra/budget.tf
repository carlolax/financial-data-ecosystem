# ==========================================
# BILLING & BUDGET ALERTS
# ==========================================

resource "google_billing_budget" "budget" {
  billing_account = var.billing_account_id
  display_name    = "Crypto-Data-Platform-Budget-Watchdog"

  amount {
    specified_amount {
      units = "10" 
    }
  }

  # Alert at 50% ($5.00)
  threshold_rules {
    threshold_percent = 0.5
  }

  # Alert at 90% ($9.00)
  threshold_rules {
    threshold_percent = 0.9
  }

  # Alert at 100% ($10.00)
  threshold_rules {
    threshold_percent = 1.0
  }
}
