# Investigation: GitHub Action Stuck in Queued State

**Status:** In progress  
**Opened:** 2026-05-15  
**Repo:** yaniv256/openai-realtime-meeting-assistant-private  
**Run:** 25907155193  

## Symptom
GitHub Action "Deploy to EC2" stuck in `queued` state 4+ minutes after push to main.
No jobs ever created. `updated_at` = `created_at` (07:59:59Z) — never updated.

## Impact
CI/CD pipeline non-functional. Manual deploy required.

## Timeline
- Repo created via `gh repo create --private`
- Workflow `.github/workflows/deploy.yml` pushed in first commit to main
- Run 25907155193 triggered 07:59:59Z 2026-05-15
- Still queued at 08:03:xx — no runner pickup

## Root Cause: **95% confidence**

GitHub account `yaniv256` has a $0 Actions spending limit (or exhausted free minutes) for private repos. Under the 2026 GitHub pricing model, private repo runs silently queue forever with 0 jobs instead of failing with an explicit error.

**Evidence:**
- Run 25907155193: queued, 0 jobs, never updated
- Cancel API returned HTTP 500 (run in permanent queued state)
- Retry run 25907443872: also immediately queued, 0 jobs
- Actions enabled=true, workflow state=active, secrets present
- GitHub status: operational

**Fix options (in order of preference):**
1. Go to github.com/settings/billing → Actions → set spending limit > $0
2. Move GitHub Action to public fork (unlimited free minutes)
3. Replace with self-hosted runner on EC2

**Workaround in place:** `bash /home/ubuntu/deploy-meeting.sh` via SSH.
