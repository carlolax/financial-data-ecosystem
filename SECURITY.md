# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

I take security seriously. If you discover a security vulnerability within this project, please follow these steps:

1. **Do NOT create a public GitHub Issue.** Publicly logging a vulnerability can allow malicious actors to exploit it before a fix is released.
2. Send an email to carlo.laxamana@gmail.com with the subject line `Security Vulnerability - Crypto Data Platform`.
3. Include a description of the issue and steps to reproduce it.

I will review the issue and respond within 48 hours or less.

## Infrastructure Security
This project uses **Terraform** to manage infrastructure. Security features include:
- **Least Privilege:** Service Accounts are restricted to specific roles (e.g., `roles/storage.objectAdmin`).
- **Secret Management:** Sensitive keys are stored in GitHub Secrets, not in the codebase.
- **Budget Alerts:** Automated monitoring prevents resource abuse.