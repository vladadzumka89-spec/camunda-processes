---
name: status
description: "📊 Показати статус всіх активних CI/CD процесів"
---

# 📊 Status — статус CI/CD процесів

Покажи користувачу статус всіх активних CI/CD процесів. Виконай наступні команди і покажи результат в зручному форматі.

## Крок 1: Отримай токен і дані

```bash
source /opt/camunda/docker-compose-8.8/.env.camunda 2>/dev/null
TOKEN=$(curl -s -X POST "http://localhost:18080/auth/realms/camunda-platform/protocol/openid-connect/token" \
  -d "client_id=${ZEEBE_CLIENT_ID}" \
  -d "client_secret=${ZEEBE_CLIENT_SECRET}" \
  -d "grant_type=client_credentials" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

python3 -c "
import json, requests

TOKEN = '${TOKEN}'
BASE = 'http://localhost:8088'
H = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

labels = {
    'catch_pr_created': '⏳ Чекає PR',
    'catch_review_done': '🔍 Чекає ревʼю',
    'catch_pr_updated': '✏️ Чекає push (rework)',
    'ST_merge_to_staging': '🔀 Merge в staging',
    'catch_staging_deployed': '🚀 Чекає деплой staging',
    'GW_event_staging': '🧪 Перевірка staging',
    'GW_deploy_ok': '⚙️ Перевірка deploy',
    'ST_comment_deploy_failed': '❌ Deploy failed',
    'ST_comment_rework': '💬 Коментар rework',
    'ST_create_merge_task': '📋 Задача merge',
    'catch_pr_merged': '⏳ Чекає merge PR',
    'catch_prod_deployed': '🌙 Чекає деплой production',
    'ST_create_odoo': '📝 Створення задачі',
}

# FTP
resp = requests.post(f'{BASE}/v2/process-instances/search', headers=H, json={
    'filter': {'processDefinitionId': 'feature-to-production', 'state': 'ACTIVE'},
    'sort': [{'field': 'startDate', 'order': 'DESC'}],
    'page': {'limit': 50},
})
items = resp.json().get('items', [])

print('## Feature to Production')
print()
if not items:
    print('Немає активних процесів')
else:
    for i in items:
        pik = i['processInstanceKey']
        v = i['processDefinitionVersion']
        inc = '❌' if i['hasIncident'] else '  '

        elem = requests.post(f'{BASE}/v2/element-instances/search', headers=H, json={
            'filter': {'processInstanceKey': str(pik), 'state': 'ACTIVE'}}).json()
        el = elem.get('items', [{}])[0].get('elementId', '???') if elem.get('items') else '???'

        pr = requests.post(f'{BASE}/v2/variables/search', headers=H, json={
            'filter': {'processInstanceKey': str(pik), 'name': 'pr_number'}}).json()
        prn = pr['items'][0]['value'].strip('\"') if pr.get('items') else '???'

        br = requests.post(f'{BASE}/v2/variables/search', headers=H, json={
            'filter': {'processInstanceKey': str(pik), 'name': 'head_branch'}}).json()
        branch = br['items'][0]['value'].strip('\"') if br.get('items') else '???'

        label = labels.get(el, el)
        link = f'http://camunda-demo.a.local:8088/operate/processes/{pik}'
        print(f'{inc} **PR #{prn}** \`{branch}\` — {label}')
        print(f'   {link}')
        print()

# Deploy-scheduler
resp = requests.post(f'{BASE}/v2/process-instances/search', headers=H, json={
    'filter': {'processDefinitionId': 'deploy-scheduler', 'state': 'ACTIVE'}})
ds = resp.json().get('items', [])
print('## Deploy Scheduler')
print()
if not ds:
    print('Вільний ✅')
else:
    for i in ds:
        inc = '❌ INCIDENT' if i['hasIncident'] else '🔄 Працює'
        print(f'{inc} (started {i[\"startDate\"][:19]})')
print()

# Nightly deploy
resp = requests.post(f'{BASE}/v2/process-instances/search', headers=H, json={
    'filter': {'processDefinitionId': 'production-nightly-deploy', 'state': 'ACTIVE'}})
nd = resp.json().get('items', [])
print('## Nightly Production Deploy')
print()
if not nd:
    print('Вільний ✅ (наступний запуск о 23:30)')
else:
    for i in nd:
        inc = '❌ INCIDENT' if i['hasIncident'] else '🔄 Працює'
        print(f'{inc} (started {i[\"startDate\"][:19]})')
"
```

## Крок 2: Покажи результат

Покажи вивід скрипта як є — це вже форматований markdown.
