# Security Policy

## Supported Versions

We release patches for security vulnerabilities for the following versions:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take the security of Spark Plan Viz seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please DO NOT

- Open a public GitHub issue for security vulnerabilities
- Discuss the vulnerability in public forums, social media, or mailing lists

### Please DO

**Report security vulnerabilities privately using one of these methods:**

1. **GitHub Security Advisories (Recommended)**
   - Go to the [Security tab](https://github.com/montanarograziano/spark_plan_viz/security/advisories)
   - Click "Report a vulnerability"
   - Provide detailed information about the vulnerability

2. **Email**
   - Send an email to the repository maintainers
   - Include "SECURITY" in the subject line
   - Provide detailed information about the vulnerability

### What to Include

When reporting a vulnerability, please include:

- **Description**: Detailed description of the vulnerability
- **Impact**: What could an attacker accomplish?
- **Reproduction Steps**: Step-by-step instructions to reproduce the issue
- **Affected Versions**: Which versions are affected?
- **Suggested Fix**: If you have ideas for how to fix it
- **Your Contact Info**: How we can reach you for follow-up

### What to Expect

1. **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
2. **Assessment**: We will assess the vulnerability and determine its severity
3. **Fix Development**: We will work on a fix and keep you informed of progress
4. **Disclosure**: Once a fix is available, we will:
   - Release a security patch
   - Publish a security advisory
   - Credit you (if desired) for the discovery

### Security Update Process

When a security vulnerability is confirmed:

1. A fix is developed in a private repository
2. A security advisory is prepared
3. A new version is released with the fix
4. The security advisory is published
5. Users are notified through GitHub and release notes

## Security Best Practices for Users

When using Spark Plan Viz:

1. **Keep Updated**: Always use the latest version
2. **Review Dependencies**: Check that PySpark and other dependencies are up to date
3. **Trusted Data**: Only visualize execution plans from trusted data sources
4. **Environment**: Be cautious when running in shared or untrusted environments

## Scope

This security policy applies to:

- The `spark_plan_viz` Python package
- Official documentation and examples
- GitHub repository infrastructure

## Known Security Considerations

### HTML Output
- The tool generates HTML files that include user data
- Be cautious when sharing generated HTML files as they may contain sensitive information about your data structures

### JavaScript Dependencies
- The visualization uses D3.js from CDN
- Ensure you trust the CDN source or consider hosting D3.js locally

## Questions?

If you have questions about this security policy, please open a discussion in the [Discussions](https://github.com/montanarograziano/spark_plan_viz/discussions) section.

---

**Last Updated**: November 2025
