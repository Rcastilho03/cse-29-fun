# Relatório de Projeto — Skill `brasa-bulk-import`

**Projeto:** Skill Claude Code para importação assíncrona de membros via Lambda + SQS + EventBridge
**Volume:** 24.000 registros para DEV (staging) + ~24.000 para PROD posteriormente
**Padrão de uso:** Jobs particionados manualmente em CSVs de ~3K registros, agendados em janelas noturnas
**Stack alvo:** Django REST (endpoint `/import-master-csv/` já implementado) + AWS (Lambda + SQS + S3 + EventBridge Scheduler)
**Operadores previstos:** Gerente Interino de Tecnologia + Gerentes de Dados e Tech (acesso restrito)
**Autor:** Ricardo Castilho, Gerente Interino de Tecnologia BRASA
**Data:** Maio 2026

---

## Sumário

1. Contexto e motivação
2. Arquitetura técnica
3. Especificação da skill Claude Code
4. Setup de infraestrutura AWS
5. Runbook operacional — DEV (staging)
6. Runbook operacional — PROD
7. Plano de testes e validação
8. Prompt para Claude Code criar a skill
9. Riscos, mitigações e decisões
10. Apêndices

---

## 1. Contexto e motivação

### 1.1 Situação atual

A BRASA tem um endpoint `/import-master-csv/` no backend Django REST que recebe um CSV de membros, processa cada linha (criação/upsert de User + BLUsers + dados relacionados) e retorna um ZIP com `summary.json`, `report_usuarios.csv` e `report_conferencias.csv`.

O endpoint é **idempotente, completo, validado e testado**. Não há intenção de modificá-lo.

Em testes empíricos, **o endpoint processa ~700 usuários em 20 minutos** (~0,58 linhas/segundo). Esse throughput é aceitável dada a complexidade das criações em banco.

### 1.2 Problema

Para migrar 24.000 registros legados:
- Tempo total de processamento contínuo: ~11h30
- Rodar do laptop pessoal é **inviável**: queda de internet, sleep, timeout HTTP, qualquer evento interrompe o processo
- Janelas noturnas têm duração limitada (2h em PROD)
- Solução precisa ser **assíncrona, rodando na nuvem, sem depender da máquina do operador**

### 1.3 Decisão arquitetural

A migração será feita em **múltiplas janelas**, cada uma processando ~3K registros:
- **DEV (staging)**: pode rodar vários jobs por dia se conveniente, sem janela rígida
- **PROD**: 1 job por noite, durante ~8 noites, em janela noturna de 2h

Cada job é uma execução independente, identificada por `job_id` único, com seu próprio prefixo S3, sua própria fila lógica de mensagens SQS, e seu próprio relatório agregado final.

### 1.4 Por que skill no Claude Code

A skill resolve dois problemas operacionais:

1. **Confiabilidade operacional**: comandos AWS CLI manuais para split, upload, enfileiramento, agendamento e monitoramento são propensos a erro humano. A skill encapsula isso como fluxo testado.

2. **Continuidade institucional**: dada a rotatividade de 13 meses no time SWE, a skill se torna **conhecimento institucional codificado**. Próximo Gerente de Dados/Tech opera sem precisar reaprender AWS.

---

## 2. Arquitetura técnica

### 2.1 Visão geral do fluxo

```
[Operador]
    ↓ "Claude, agende o arquivo X para 3h"
[Claude Code + skill brasa-bulk-import]
    ↓ executa internamente:
    1. Valida CSV (reaproveita validação do import_conf.md atual)
    2. Divide em batches de 250 linhas
    3. Upload de cada batch para S3 (s3://brasa-migrations/jobs/{job_id}/inputs/)
    4. Cria EventBridge Schedule one-time para horário X
    5. Persiste metadados do job em S3 (s3://.../jobs/{job_id}/manifest.json)
    ↓ aguarda execução agendada
[EventBridge Scheduler @ 03:00 BRT]
    ↓ dispara Lambda "brasa-bulk-import-orchestrator"
[Lambda orchestrator]
    ↓ lê manifest.json do S3
    ↓ enfileira N mensagens SQS (uma por batch)
[SQS Queue]
    ↓ entrega para
[Lambda worker "brasa-bulk-import-worker"]  
    ↓ para cada mensagem:
    1. Baixa batch_NNN.csv do S3
    2. Lê JWT da env var do próprio Lambda (atualizada via skill `refresh-token`)
    3. POST para /import-master-csv/ no endpoint configurado
    4. Salva ZIP de resposta em s3://.../jobs/{job_id}/outputs/batch_NNN.zip
    5. Marca batch como concluído em s3://.../jobs/{job_id}/state/batch_NNN.done
    ↓ retry automático em falhas (até 3x)
    ↓ falhas finais vão para DLQ (Dead Letter Queue)
    ↓ ao fim:
[Operador @ manhã seguinte]
    ↓ "Claude, status do job dev-2026-05-17-noite1"
[Skill consulta S3 + SQS + Lambda logs]
    ↓ "98/96 batches concluídos, 1 em DLQ. Quer ver detalhes?"
```

### 2.2 Diagrama da arquitetura


```
┌─────────────────────────────────────────────────────────────────────┐
│                         OPERADOR (Ricardo)                           │
│                              │                                       │
│                              ▼                                       │
│                    ┌──────────────────────┐                          │
│                    │  Claude Code +       │                          │
│                    │  skill brasa-bulk-   │                          │
│                    │  import              │                          │
│                    └──────────┬───────────┘                          │
└─────────────────────────────┼─────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────────┐
        │ AWS — conta da BRASA                             │
        │                                                   │
        │   ┌──────────────────────────────────────────┐  │
        │   │ S3: brasa-migrations                      │  │
        │   │   /jobs/{job_id}/                         │  │
        │   │     manifest.json                         │  │
        │   │     inputs/batch_001.csv...batch_NNN.csv  │  │
        │   │     outputs/batch_001.zip...              │  │
        │   │     state/batch_001.done...               │  │
        │   │     report_final.json                     │  │
        │   └──────────────────────────────────────────┘  │
        │                                                   │
        │   ┌──────────────────────────────────────────┐  │
        │   │ EventBridge Scheduler                     │  │
        │   │ one-time schedule por job                 │  │
        │   └──────────────┬───────────────────────────┘  │
        │                  │ dispara                       │
        │                  ▼                                │
        │   ┌──────────────────────────────────────────┐  │
        │   │ Lambda: orchestrator                      │  │
        │   │ - lê manifest                             │  │
        │   │ - enfileira N msgs SQS                    │  │
        │   └──────────────┬───────────────────────────┘  │
        │                  │                                │
        │                  ▼                                │
        │   ┌──────────────────────────────────────────┐  │
        │   │ SQS: brasa-bulk-import-queue              │  │
        │   │ + Dead Letter Queue                       │  │
        │   └──────────────┬───────────────────────────┘  │
        │                  │ consumer                      │
        │                  ▼                                │
        │   ┌──────────────────────────────────────────┐  │
        │   │ Lambda: worker (concurrency=1 padrão)     │  │
        │   │ - HTTP POST para endpoint                 │  │
        │   │ - salva ZIP em S3                         │  │
        │   └──────────────┬───────────────────────────┘  │
        │                  │ HTTPS                          │
        └──────────────────┼─────────────────────────────────┘
                           │
                           ▼
       ┌────────────────────────────────────────────┐
       │ Django REST endpoint                        │
       │ staging: portalbrasa-staging.gobrasa.org    │
       │ prod:    portalbrasa.gobrasa.org            │
       │                                             │
       │ POST /import-master-csv/                    │
       │ Auth: Bearer JWT                            │
       │ Response: ZIP (summary + reports)           │
       └────────────────────────────────────────────┘
```

### 2.3 Componentes detalhados

#### 2.3.1 S3 — Estrutura de buckets

**Bucket único:** `brasa-migrations` (região `us-east-1`, sugestão)

**Estrutura por job:**
```
s3://brasa-migrations/
└── jobs/
    └── {job_id}/                       # ex: dev-2026-05-17-noite1
        ├── manifest.json                # metadados do job
        ├── inputs/
        │   ├── batch_001.csv
        │   ├── batch_002.csv
        │   └── ... batch_NNN.csv
        ├── outputs/
        │   ├── batch_001.zip            # resposta do endpoint
        │   └── ...
        ├── state/
        │   ├── batch_001.done           # arquivo vazio = sucesso
        │   ├── batch_002.failed         # contém erro
        │   └── ...
        └── report_final.json            # gerado após conclusão
```

**Formato do `manifest.json`:**
```json
{
  "job_id": "dev-2026-05-17-noite1",
  "target_environment": "staging",
  "target_endpoint": "https://portalbrasa-staging.gobrasa.org/import-master-csv/",
  "total_rows": 3000,
  "batch_size": 250,
  "total_batches": 12,
  "created_at": "2026-05-17T14:32:11-03:00",
  "scheduled_for": "2026-05-17T03:00:00-03:00",
  "lambda_concurrency": 1,
  "jwt_last_refreshed_at": "2026-05-17T13:00:00-03:00",
  "status": "scheduled"
}
```

> **Sobre `jwt_last_refreshed_at`:** registra quando o JWT na env var do Lambda foi atualizado pela última vez. A skill usa esse campo para alertar se o token está próximo de expirar (>2 dias desde refresh).

#### 2.3.2 EventBridge Scheduler

**Schedule one-time por job:**
- Nome: `brasa-bulk-import-{job_id}`
- Tipo: `at()` expression (one-time)
- Timezone: `America/Sao_Paulo`
- Target: Lambda `brasa-bulk-import-orchestrator`
- Payload: `{"job_id": "{job_id}"}`
- ActionAfterCompletion: `DELETE` (autodestroi)

**Comando AWS CLI exemplo:**
```bash
aws scheduler create-schedule \
  --name "brasa-bulk-import-dev-2026-05-17-noite1" \
  --schedule-expression "at(2026-05-17T03:00:00)" \
  --schedule-expression-timezone "America/Sao_Paulo" \
  --flexible-time-window '{"Mode": "OFF"}' \
  --target '{
    "Arn": "arn:aws:lambda:us-east-1:ACCOUNT_ID:function:brasa-bulk-import-orchestrator",
    "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/EventBridgeSchedulerToLambdaRole",
    "Input": "{\"job_id\": \"dev-2026-05-17-noite1\"}"
  }' \
  --action-after-completion DELETE
```

#### 2.3.3 SQS

**Fila principal:** `brasa-bulk-import-queue`
- Visibility timeout: 16 minutos (Lambda timeout 15min + 1min de buffer)
- Message retention: 4 dias
- Max receive count: 3 (após 3 falhas, vai para DLQ)
- Long polling: 20 segundos

**Dead Letter Queue:** `brasa-bulk-import-dlq`
- Message retention: 14 dias
- Para análise manual de batches que falharam 3x

**Formato da mensagem:**
```json
{
  "job_id": "dev-2026-05-17-noite1",
  "batch_number": "001",
  "s3_key": "jobs/dev-2026-05-17-noite1/inputs/batch_001.csv",
  "target_endpoint": "https://portalbrasa-staging.gobrasa.org/import-master-csv/"
}
```

> O JWT não vai na mensagem — o Lambda worker lê de sua própria variável de ambiente `BRASA_TARGET_JWT`. Isso mantém a mensagem SQS livre de credenciais.

#### 2.3.4 Lambda — orchestrator

**Função:** `brasa-bulk-import-orchestrator`
- Runtime: Python 3.12
- Timeout: 1 minuto
- Memória: 256 MB
- Trigger: EventBridge Scheduler

**Responsabilidade:**
1. Recebe `{"job_id": "..."}`
2. Lê `s3://brasa-migrations/jobs/{job_id}/manifest.json`
3. Para cada batch de 1 a N, enfileira mensagem SQS
4. Atualiza `manifest.json` com `status: "running"` e `dispatched_at`

#### 2.3.5 Lambda — worker

**Função:** `brasa-bulk-import-worker`
- Runtime: Python 3.12
- Timeout: 15 minutos (máximo Lambda)
- Memória: 512 MB
- Trigger: SQS (event source mapping)
- Reserved concurrency: **1** (padrão; ajustável por job)
- Batch size SQS: 1 (processa 1 batch por invocação)

**Responsabilidade:**
1. Recebe mensagem SQS com metadata do batch
2. Baixa CSV do S3
3. Obtém JWT da env var `BRASA_TARGET_JWT` (atualizada pelo operador via skill)
4. POST multipart/form-data para o endpoint
5. Salva ZIP em `s3://.../jobs/{job_id}/outputs/batch_NNN.zip`
6. Cria marcador `state/batch_NNN.done` ou `.failed`
7. Em caso de erro: re-lança exception (SQS retenta automaticamente)
8. Em caso de erro 401 (token expirado): falha imediata sem retry, alerta clara no log

#### 2.3.6 Gestão de JWT (sem Secrets Manager)

O backend BRASA gera tokens JWT via execução de shell no container Django:

```bash
docker exec brasa_backend_dev python core/manage.py shell -c "
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import get_user_model
user = get_user_model().objects.filter(is_superuser=True).first()
refresh = RefreshToken.for_user(user)
print(str(refresh.access_token))
"
```

O token gerado tem **3 dias de validade**, o que cobre folgadamente qualquer janela de execução.

**Estratégia:** o token é armazenado como **variável de ambiente do Lambda worker**, atualizada manualmente pelo operador antes de cada janela (ou periodicamente a cada 2 dias para garantir margem).

**Por que NÃO usar Secrets Manager:**
- O token só pode ser gerado via shell no Docker, não há endpoint HTTP para Lambda buscar dinamicamente
- 3 dias de validade dá margem confortável para gestão manual
- Reduz complexidade e custo
- O fluxo `aws lambda update-function-configuration` é simples e auditável

**Fluxo de rotação do JWT antes de cada janela:**

```bash
# 1) Gere o token no container (mesma execução do import_conf.md atual)
TOKEN=$(docker exec brasa_backend_dev python core/manage.py shell -c "
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import get_user_model
user = get_user_model().objects.filter(is_superuser=True).first()
refresh = RefreshToken.for_user(user)
print(str(refresh.access_token))
" | tail -1)

# 2) Atualize a env var do Lambda worker (staging)
aws lambda update-function-configuration \
  --function-name brasa-bulk-import-worker \
  --environment "Variables={BRASA_TARGET_JWT=$TOKEN,BRASA_TARGET_ENV=staging}"

# 3) Para PROD, mesmo fluxo, com container/usuário equivalente
```

A skill `brasa-bulk-import` inclui um comando dedicado `refresh-token` que automatiza esse processo, e o comando `prepare` valida que o token no Lambda foi atualizado nas últimas 48 horas antes de permitir o agendamento.

---

## 3. Especificação da skill Claude Code

### 3.1 Identidade da skill

- **Nome:** `brasa-bulk-import`
- **Localização:** `~/.claude/skills/brasa-bulk-import/`
- **Descrição:** "Importa CSVs grandes de membros para o backend BRASA via pipeline assíncrono Lambda+SQS. Use quando o operador mencionar 'enviar CSV em lote', 'agendar import', 'job de migração de membros', ou referir-se ao endpoint /import-master-csv/ com volume > 500 linhas."

### 3.2 Estrutura de arquivos

```
~/.claude/skills/brasa-bulk-import/
├── SKILL.md                            # arquivo principal lido pelo Claude
├── scripts/
│   ├── validate_csv.py                 # baseado no import_conf.md existente
│   ├── split_csv.py                    # divide CSV em batches de 250
│   ├── upload_job.py                   # cria estrutura S3 e manifest
│   ├── schedule_job.py                 # cria EventBridge schedule
│   ├── run_job_now.py                  # dispara orchestrator imediatamente
│   ├── status_job.py                   # consulta estado de um job
│   ├── aggregate_report.py             # agrega ZIPs em relatório final
│   ├── cancel_job.py                   # cancela job agendado/em execução
│   └── refresh_token.py                # gera JWT via docker exec + atualiza Lambda env var
├── lambdas/
│   ├── orchestrator/
│   │   ├── handler.py
│   │   ├── requirements.txt
│   │   └── deploy.sh
│   └── worker/
│       ├── handler.py
│       ├── requirements.txt
│       └── deploy.sh
├── infra/
│   ├── setup_aws.sh                    # cria buckets, queues, IAM, lambdas
│   ├── teardown_aws.sh                 # remove infra (para testes)
│   └── policies/
│       ├── lambda_worker_role.json
│       ├── orchestrator_role.json
│       └── eventbridge_role.json
├── config/
│   ├── environments.json               # endpoints de staging e prod
│   └── last_token_refresh.json         # timestamp do último refresh (gerado dinamicamente)
└── README.md                            # documentação para humanos
```

### 3.3 Comandos da skill (modo híbrido)

O operador pode invocar de duas formas:

**Modo conversacional (preferido):**
> "Claude, prepare o arquivo `/Users/Ricardo/Downloads/membros_legado_lote_3.csv` para enviar amanhã às 3h da manhã para staging"

A skill interpreta e executa internamente o equivalente a:
```bash
brasa-bulk-import prepare /Users/Ricardo/Downloads/membros_legado_lote_3.csv \
  --target=staging \
  --job-id=dev-2026-05-18-noite3 \
  --schedule="2026-05-18T03:00:00-03:00"
```

**Durante a execução, Claude deve dar feedback em tempo real:**
```
🔍 Validando CSV...
   ✓ 3.000 linhas detectadas
   ✓ Encoding UTF-8 confirmado
   ✓ Colunas obrigatórias presentes
   ✓ Sem espaços em headers
   ✓ Datas no formato esperado
   
📂 Dividindo em batches de 250 linhas...
   ✓ 12 batches gerados em /tmp/brasa-jobs/dev-2026-05-18-noite3/

☁️  Subindo para S3...
   ✓ batch_001.csv (250 linhas, 47KB)
   ✓ batch_002.csv (250 linhas, 47KB)
   ... [progresso linha a linha]
   ✓ batch_012.csv (250 linhas, 47KB)
   ✓ manifest.json criado

⏰ Agendando execução para 18/05/2026 03:00 BRT...
   ✓ EventBridge schedule "brasa-bulk-import-dev-2026-05-18-noite3" criado
   ✓ Schedule expira e se autodestroi após execução

✅ Job preparado e agendado.

Resumo:
  Job ID:           dev-2026-05-18-noite3
  Target:           staging (portalbrasa-staging.gobrasa.org)
  Total de linhas:  3.000
  Total de batches: 12
  Agendado para:    18/05/2026 às 03:00 BRT
  Concurrency:      1 (sequencial)
  Tempo estimado:   ~85 minutos
  
Para consultar status amanhã: "Claude, status do job dev-2026-05-18-noite3"
Para cancelar: "Claude, cancele o job dev-2026-05-18-noite3"
```

**Comandos principais disponíveis:**

| Comando | O que faz | Quando usar |
|---------|-----------|-------------|
| `refresh-token` | Gera novo JWT via docker exec + atualiza env var do Lambda | Antes de cada janela / a cada 2 dias |
| `prepare` | Valida + divide + sobe S3 + cria manifest + verifica idade do JWT | Sempre primeiro passo |
| `schedule` | Cria EventBridge schedule para job preparado | Após `prepare`, para execução noturna |
| `run-now` | Dispara orchestrator imediatamente (pula schedule) | Para DEV / testes |
| `status` | Consulta estado de um job | A qualquer momento |
| `report` | Gera relatório consolidado de job concluído | Após `status` mostrar 100% |
| `cancel` | Cancela job agendado (não-executado ainda) | Se precisar abortar |
| `retry-failed` | Reenfileira batches que foram para DLQ | Após análise de falhas |
| `list-jobs` | Lista todos os jobs existentes no S3 | Auditoria / overview |

### 3.4 Guardrails da skill

A skill **DEVE** ter os seguintes mecanismos de segurança:

#### 3.4.1 Confirmação obrigatória para PROD

Qualquer comando que referencie `--target=prod` ou endpoint contendo `portalbrasa.gobrasa.org` (sem o `-staging`) **DEVE** exigir confirmação explícita do operador, e a confirmação deve incluir a frase exata:

> "Confirmo execução em PROD do job {job_id} com {N} registros"

Sem isso, abortar.

#### 3.4.2 Validação de duplicidade de job_id

Antes de criar um novo job, verificar se já existe `s3://brasa-migrations/jobs/{job_id}/manifest.json`. Se existir:
- Mostrar o status do job existente
- Pedir confirmação explícita para sobrescrever OU sugerir job_id alternativo

#### 3.4.3 Limites de volume por job

- Máximo de **5.000 linhas por job** em DEV
- Máximo de **3.500 linhas por job** em PROD (respeitando janela de 2h)

Volumes acima exigem confirmação extra e justificativa.

#### 3.4.4 Verificação de horário em PROD

Para `--target=prod`, o `--schedule` deve estar entre **03:00 e 04:00 BRT**. Fora dessa janela, abortar com mensagem explicando a política.

#### 3.4.5 Estado de execução claro

A skill **NUNCA** assume que um job foi bem-sucedido. Após `schedule` ou `run-now`, ela sempre instrui o operador a consultar `status` antes de fazer novo agendamento.

#### 3.4.6 Validação de frescor do JWT

Antes de `prepare`, a skill verifica `jwt_last_refreshed_at` na configuração do Lambda. Se for `>48 horas` atrás, a skill **DEVE** alertar:

> "⚠️ O JWT no Lambda foi atualizado pela última vez há X horas. Tokens duram 3 dias. Recomendado rodar `refresh-token` antes de agendar este job, especialmente se a execução for em mais de 24 horas."

Se for `>60 horas` atrás (faltando <12h para expirar), a skill **DEVE** recusar o agendamento e exigir `refresh-token` primeiro.

### 3.5 Conteúdo do `SKILL.md`

O arquivo principal da skill deve seguir este esqueleto:

```markdown
---
name: brasa-bulk-import
description: Use this skill when the operator wants to import large CSVs (>500 rows) of members into the BRASA backend via asynchronous Lambda+SQS pipeline. Triggers on phrases like "enviar CSV em lote", "agendar import de membros", "job de migração", "bulk import", or any reference to processing CSVs with >500 lines for the /import-master-csv/ endpoint.
---

# brasa-bulk-import

Skill for asynchronously importing large CSVs of BRASA members via AWS Lambda+SQS pipeline. Used during migrations from legacy systems and for batch updates that exceed the synchronous request window.

## When to use this skill

Activate this skill when the operator mentions:
- "Enviar CSV em lote", "import em lote", "bulk import"
- "Agendar import" or "schedule a job"
- "Migrar membros legados"
- Provides a CSV file path AND mentions >500 rows OR an execution time
- Mentions "job ID" with prefix like dev-YYYY-MM-DD or prod-YYYY-MM-DD

Do NOT use this skill for:
- Single-file uploads <500 rows (use existing import_conf.md flow)
- Synchronous testing of the endpoint (use direct HTTP)
- Any operation that is not import of members CSV

## Required context before executing

Before running any command, confirm with the operator:
1. **CSV file path** (absolute path on local machine)
2. **Target environment** (staging or prod)
3. **Job ID** (suggest format: `{env}-{YYYY-MM-DD}-{descriptor}`, e.g. `dev-2026-05-17-noite1`)
4. **Execution mode**:
   - `prepare-only`: just validate, split, upload S3 (no scheduling)
   - `schedule`: prepare + schedule for specified time
   - `run-now`: prepare + execute immediately (DEV ONLY)
5. **If schedule**: target datetime in BRT (timezone America/Sao_Paulo)

## Commands

### prepare

[detailed spec for prepare command including all validation steps, splitting logic, S3 upload, manifest creation]

### schedule

[detailed spec including PROD guardrails, timezone handling, schedule naming]

### run-now

[detailed spec — DEV ONLY, never PROD]

### status

[detailed spec including how to parse S3 state files, count done/failed batches]

### report

[detailed spec for aggregating ZIPs, generating final summary]

### cancel

[detailed spec for cancelling scheduled jobs]

### retry-failed

[detailed spec for re-enqueuing DLQ messages]

### list-jobs

[detailed spec]

## Real-time feedback patterns

This skill MUST provide continuous feedback. Never execute a multi-step command silently.

Pattern for each operation:
1. State what's about to happen
2. Execute with intermediate status (use the scripts in scripts/)
3. Confirm completion or surface error clearly

[examples of well-formatted output]

## Guardrails

### PROD safety
- All PROD operations require explicit confirmation phrase
- PROD scheduling restricted to 03:00-04:00 BRT
- Volume cap of 3,500 rows per job in PROD

### Idempotency
- Check for existing job_id before creating
- Show status of existing job if conflict
- Require explicit override confirmation

### Failure visibility
- Never silently succeed
- Always report batch-level success/failure counts
- Always provide next-step suggestions

## Error handling

Common error patterns and how to handle them:
- CSV validation failure → abort, show issues, ask operator to fix
- AWS credentials missing → abort, show how to configure
- Job ID conflict → show existing job, ask for confirmation
- Lambda not deployed → abort, suggest running infra/setup_aws.sh
- Network failure during S3 upload → retry up to 3 times, then abort
- SQS quota exceeded → abort, surface AWS error

## Configuration

The skill reads from `config/environments.json`:
```json
{
  "staging": {
    "endpoint": "https://portalbrasa-staging.gobrasa.org/import-master-csv/",
    "docker_container": "brasa_backend_dev",
    "lambda_env_var": "BRASA_TARGET_JWT"
  },
  "prod": {
    "endpoint": "https://portalbrasa.gobrasa.org/import-master-csv/",
    "docker_container": "brasa_backend_prod",
    "lambda_env_var": "BRASA_TARGET_JWT"
  }
}
```

Note: same Lambda env var (`BRASA_TARGET_JWT`) is reused — the skill updates the value when switching between environments. There's a single Lambda worker; the target environment is determined by the manifest of each job.

## Dependencies

- AWS CLI v2 configured with credentials
- Python 3.12+
- boto3, requests, click (for scripts)
- jq (optional, for pretty-printing manifest.json)
```

---

## 4. Setup de infraestrutura AWS

### 4.1 Pré-requisitos

- [ ] Conta AWS da BRASA com acesso administrativo
- [ ] AWS CLI v2 instalado e configurado no laptop do operador
- [ ] Região definida: **us-east-1** (sugestão; usar mesma do backend)
- [ ] Conta verificada se está no período de free tier de 12 meses

### 4.2 Recursos a criar

#### 4.2.1 S3 Bucket

```bash
aws s3api create-bucket \
  --bucket brasa-migrations \
  --region us-east-1

aws s3api put-bucket-versioning \
  --bucket brasa-migrations \
  --versioning-configuration Status=Enabled

aws s3api put-public-access-block \
  --bucket brasa-migrations \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Lifecycle: deletar objetos com >90 dias (auditoria fica por 3 meses)
aws s3api put-bucket-lifecycle-configuration \
  --bucket brasa-migrations \
  --lifecycle-configuration file://infra/s3_lifecycle.json
```

#### 4.2.2 SQS Queues

```bash
# Dead Letter Queue primeiro
aws sqs create-queue \
  --queue-name brasa-bulk-import-dlq \
  --attributes '{
    "MessageRetentionPeriod": "1209600"
  }'

# Pega o ARN da DLQ
DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url $(aws sqs get-queue-url --queue-name brasa-bulk-import-dlq --output text) \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Fila principal com redrive policy para DLQ
aws sqs create-queue \
  --queue-name brasa-bulk-import-queue \
  --attributes '{
    "VisibilityTimeout": "960",
    "MessageRetentionPeriod": "345600",
    "ReceiveMessageWaitTimeSeconds": "20",
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"'$DLQ_ARN'\",\"maxReceiveCount\":\"3\"}"
  }'
```

#### 4.2.3 IAM Roles

**Lambda Worker Role** (`brasa-bulk-import-worker-role`):
- Permissões necessárias:
  - `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:GetQueueAttributes` na fila principal
  - `s3:GetObject` em `brasa-migrations/jobs/*/inputs/*`
  - `s3:PutObject` em `brasa-migrations/jobs/*/outputs/*` e `*/state/*`
  - CloudWatch Logs (padrão)
  - **Não precisa de** `secretsmanager:*` — JWT vem da env var do próprio Lambda

**Lambda Orchestrator Role** (`brasa-bulk-import-orchestrator-role`):
- `s3:GetObject` em `brasa-migrations/jobs/*/manifest.json`
- `s3:PutObject` em `brasa-migrations/jobs/*/manifest.json` (para atualizar status)
- `sqs:SendMessage` na fila principal
- CloudWatch Logs

**EventBridge Scheduler Role** (`EventBridgeSchedulerToLambdaRole`):
- `lambda:InvokeFunction` na função orchestrator

(JSONs completos no apêndice A.)

#### 4.2.4 Lambdas

Deploy via script (será gerado pelo Claude Code na execução do prompt final):
```bash
bash lambdas/orchestrator/deploy.sh
bash lambdas/worker/deploy.sh
```

Cada script faz:
1. `pip install -t ./build -r requirements.txt`
2. `cp handler.py ./build/`
3. `cd build && zip -r ../function.zip .`
4. `aws lambda create-function` (primeira vez) ou `update-function-code` (subsequentes)

#### 4.2.5 SQS → Lambda trigger

```bash
aws lambda create-event-source-mapping \
  --function-name brasa-bulk-import-worker \
  --batch-size 1 \
  --event-source-arn $(aws sqs get-queue-attributes \
    --queue-url $(aws sqs get-queue-url --queue-name brasa-bulk-import-queue --output text) \
    --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
```

### 4.3 Gestão do JWT (sem Secrets Manager)

Após criar o Lambda worker, configure a env var inicial com um JWT obtido manualmente:

```bash
# Gera o token via Docker (no laptop do operador)
TOKEN=$(docker exec brasa_backend_dev python core/manage.py shell -c "
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import get_user_model
user = get_user_model().objects.filter(is_superuser=True).first()
refresh = RefreshToken.for_user(user)
print(str(refresh.access_token))
" | tail -1)

# Configura no Lambda
aws lambda update-function-configuration \
  --function-name brasa-bulk-import-worker \
  --environment "Variables={BRASA_TARGET_JWT=$TOKEN}"
```

A skill encapsula esse processo no comando `refresh-token`. Em uso normal:

```
Operador: "Claude, refresh token do staging"
Claude:   [executa docker exec, captura token, atualiza Lambda env var,
           registra timestamp em config/last_token_refresh.json]
          "✅ Token atualizado às 14:32 BRT. Válido até ~14:32 de 19/05/2026."
```

### 4.4 Verificação inicial pós-setup

Após `setup_aws.sh` rodar com sucesso, valide:

```bash
# 1) Buckets existe?
aws s3 ls s3://brasa-migrations/

# 2) Filas existem?
aws sqs list-queues --queue-name-prefix brasa-bulk-import

# 3) Lambdas existem e têm o JWT configurado?
aws lambda get-function-configuration \
  --function-name brasa-bulk-import-worker \
  --query 'Environment.Variables.BRASA_TARGET_JWT' \
  --output text | head -c 20  # mostra primeiros 20 chars para validar sem expor token completo

# 4) Event source mapping ativo?
aws lambda list-event-source-mappings \
  --function-name brasa-bulk-import-worker
```

Se algum desses falhar, parar e revisar antes do primeiro smoke test.

> **Observação sobre segurança da env var:** o JWT no Lambda fica visível para qualquer pessoa com `lambda:GetFunctionConfiguration`. Restrinja esse permissionamento via IAM ao mesmo grupo de operadores da skill. Em organizações maiores, isso justificaria Secrets Manager; para BRASA com 2-3 operadores, o trade-off de simplicidade vence.

---

## 5. Runbook operacional — DEV (staging)

### 5.1 Cenário típico

Você tem `membros_legado.csv` com 24K registros. Quer fazer transferência completa para staging em DEV.

**Estratégia recomendada:**
- Lote 1 (manual): 100 registros — teste smoke
- Lote 2: 500 registros — valida pipeline completo
- Lotes 3 a 10: ~3K registros cada — paralelos ou sequenciais conforme conveniência

### 5.2 Smoke test (Lote 1, 100 registros)

```
Operador: "Claude, prepare /Users/Ricardo/Downloads/teste_100.csv para staging, 
         job dev-2026-05-17-smoke, executar agora"

Claude:   [valida, divide em 1 batch de 100, sobe S3, dispara]
          [aguarda 3-5 minutos]
          [reporta sucesso]

Operador: "Claude, status do job dev-2026-05-17-smoke"

Claude:   [mostra: 1/1 batch done, baixa ZIP, mostra summary.json]
```

Critério de sucesso: `summary.json` mostra 100 sucessos, 0 falhas.

### 5.3 Validação pipeline completo (Lote 2, 500 registros)

```
Operador: "Claude, prepare /Users/Ricardo/Downloads/teste_500.csv para staging,
         job dev-2026-05-17-pipeline, executar agora"
```

Critério: 2 batches de 250 cada, ambos completam. Tempo total ~15-20min.

### 5.4 Lotes de produção em DEV (3K cada)

```
Operador: "Claude, prepare /Users/Ricardo/Downloads/lote_3k_v1.csv para staging,
         job dev-2026-05-18-noite1, agendar para 18/05/2026 às 03:00 BRT"

[no dia seguinte de manhã]

Operador: "Claude, status do job dev-2026-05-18-noite1"
Claude:   [12/12 batches done. Quer ver report?]

Operador: "Sim"
Claude:   [agrega ZIPs, mostra summary consolidado, salva em /Users/Ricardo/...]
```

Em DEV pode rodar **vários jobs no mesmo dia** sem problema — basta usar job_ids diferentes.

### 5.5 Tratamento de falhas em DEV

Se algum batch falhar (vai para DLQ):

```
Operador: "Claude, status do job dev-2026-05-18-noite1"
Claude:   "11/12 batches done, 1 batch (batch_007) falhou após 3 tentativas.
          Posso mostrar o erro?"

Operador: "Sim"
Claude:   [mostra erro do CloudWatch Logs]

Operador: "Claude, retry o batch que falhou"
Claude:   [reenfileira batch_007, monitora, reporta]
```

---

## 6. Runbook operacional — PROD

### 6.1 Diferenças críticas vs DEV

| Aspecto | DEV (staging) | PROD |
|---------|---------------|------|
| Endpoint | portalbrasa-staging.gobrasa.org | portalbrasa.gobrasa.org |
| JWT | env var no Lambda, refresh manual | env var no Lambda, refresh manual (mesmo padrão) |
| Container origem do JWT | brasa_backend_dev | brasa_backend_prod (ou equivalente) |
| Janela | flexível, mesmo dia | 03:00–04:00 BRT obrigatório |
| Volume por job | até 5.000 | até 3.500 |
| Confirmação | normal | frase exata obrigatória |
| Snapshot RDS pré-job | recomendado | **obrigatório** |
| Backup plan | não crítico | obrigatório |
| Pessoas online | só você | você + 1 backup mínimo |
| Página de manutenção | não | **obrigatória** |
| Refresh do JWT | a cada 2 dias | obrigatório imediatamente antes da janela |

### 6.2 Cronograma sugerido para 24K em PROD

| Noite | Lote | Volume | Job ID | Acumulado |
|-------|------|--------|--------|-----------|
| 1 | Smoke | 50 | prod-2026-XX-XX-smoke | 50 |
| 2 | Validação | 500 | prod-2026-XX-XX-validacao | 550 |
| 3 | Onda 1 | 3.500 | prod-2026-XX-XX-onda1 | 4.050 |
| 4 | Onda 2 | 3.500 | prod-2026-XX-XX-onda2 | 7.550 |
| 5 | Onda 3 | 3.500 | prod-2026-XX-XX-onda3 | 11.050 |
| 6 | Onda 4 | 3.500 | prod-2026-XX-XX-onda4 | 14.550 |
| 7 | Onda 5 | 3.500 | prod-2026-XX-XX-onda5 | 18.050 |
| 8 | Onda 6 | 3.500 | prod-2026-XX-XX-onda6 | 21.550 |
| 9 | Onda 7 | 2.500 | prod-2026-XX-XX-onda7 | 24.050 |
| 10 | Reserva | DLQ retries | conforme necessidade | — |

**Total: 8 noites úteis + 2 noites de buffer = 10 noites calendário.**

### 6.3 Procedimento padrão por noite de PROD

**T-24h:**
- [ ] Preparar CSV do lote noturno (3.500 linhas máximo)
- [ ] Notificar time tech sobre janela
- [ ] Confirmar que o container do backend PROD está acessível para `refresh-token`

**T-2h (~01:00 BRT):**
- [ ] Snapshot RDS manual: `aws rds create-db-snapshot ...`
- [ ] Aguardar snapshot completar
- [ ] Ativar página de manutenção

**T-1h (~02:00 BRT):**
- [ ] Conectar com backup no Slack
- [ ] **Rodar `refresh-token` para PROD** (gera novo JWT, atualiza Lambda)
- [ ] Rodar smoke test em staging com mesmo CSV
- [ ] Se smoke OK: prosseguir

**T-30min (~02:30 BRT):**
```
Operador: "Claude, prepare /Users/Ricardo/Downloads/lote_prod_onda1.csv 
         para PROD, job prod-2026-XX-XX-onda1, agendar para hoje às 03:00 BRT"

Claude:   [valida, mostra resumo]
          "⚠️ Você está prestes a agendar um job em PRODUÇÃO.
          
           Job ID:           prod-2026-XX-XX-onda1
           Target:           portalbrasa.gobrasa.org (PROD)
           Total de linhas:  3.500
           Total de batches: 14
           Agendado para:    XX/XX/2026 03:00 BRT
           
           Para confirmar, digite exatamente:
           'Confirmo execução em PROD do job prod-2026-XX-XX-onda1 com 3500 registros'"

Operador: [digita frase exata]
Claude:   [agenda, confirma]
```

**T-0 (03:00 BRT) — execução automática.**

**T+90min (~04:30 BRT):**
- [ ] `status` job
- [ ] Se 100%: smoke test no portal (login, dashboard, busca)
- [ ] Desativar página de manutenção
- [ ] Confirmar no Slack que noite foi OK

**Manhã seguinte:**
- [ ] `report` job
- [ ] Revisar `summary.json` com time
- [ ] Tratar erros se houver
- [ ] Definir próxima noite

### 6.4 Critério de abort durante PROD

A execução em PROD deve ser abortada e rollback iniciado se:

- Taxa de falha por batch > 10% (ex: 2+ batches em 14 falhando)
- Qualquer erro de FK ou corrupção de dados visível
- Latência por batch > 20min (indica problema no backend)
- Backend retorna 5xx > 3 vezes consecutivas

**Procedimento de abort:**
```
Operador: "Claude, CANCELE o job prod-2026-XX-XX-onda1 IMEDIATAMENTE"
Claude:   [purge da fila SQS, desativa event source mapping do Lambda]
          [reporta: N batches já processados, M+ em fila cancelados]
```

Após abort, avaliar com calma se rollback do snapshot é necessário.

---

## 7. Plano de testes e validação

### 7.1 Testes da skill antes do primeiro uso real

**Teste 1: Validação de CSV inválido**
```
Arquivo: CSV com header faltando coluna obrigatória
Esperado: skill aborta, mostra problema, não cria job
```

**Teste 2: Job ID duplicado**
```
Cenário: criar dois jobs com mesmo ID
Esperado: segundo job mostra status do primeiro e pede confirmação
```

**Teste 3: Tentativa de PROD sem frase**
```
Cenário: operador tenta agendar PROD respondendo "sim" em vez da frase exata
Esperado: skill recusa, repete instrução
```

**Teste 4: Tentativa de PROD fora da janela**
```
Cenário: agendar PROD para 22:00 BRT
Esperado: skill recusa
```

**Teste 5: Run-now em PROD**
```
Cenário: tentar `run-now` com target=prod
Esperado: skill recusa categoricamente
```

**Teste 6: Smoke real em staging**
```
Cenário: CSV de 10 linhas em staging, run-now
Esperado: pipeline completo funciona, ZIP retornado
```

### 7.2 Critérios de "skill pronta para uso"

- [ ] Todos os 6 testes acima passam
- [ ] Operador (Ricardo) executou ao menos 1 job de 500+ linhas com sucesso em staging
- [ ] Tempo médio por batch medido e batendo com estimativa
- [ ] DLQ verificado vazio após smoke tests
- [ ] CloudWatch Logs do worker mostram logs claros, sem stack traces inesperados
- [ ] Documento `README.md` da skill revisado e completo

---

## 8. Prompt para Claude Code criar a skill

> **Instruções para você, Ricardo:** copie o bloco abaixo e cole no Claude Code em uma sessão nova, com este relatório como contexto (`@RELATORIO_SKILL_BULK_IMPORT.md`). Execute as fases em ordem, revisando o output entre elas.

### 8.1 Prompt mestre (cole no Claude Code)

```
Vou criar uma skill nova chamada `brasa-bulk-import` no Claude Code, conforme 
especificado em @RELATORIO_SKILL_BULK_IMPORT.md.

A skill faz import assíncrono de CSVs grandes para o endpoint /import-master-csv/ 
do backend BRASA usando AWS Lambda + SQS + EventBridge Scheduler.

Vou executar a criação em 5 fases. Por favor confirme cada fase antes de prosseguir 
e me peça aprovação antes de executar comandos AWS reais.

## Fase 1 — Estrutura local da skill

Crie a estrutura de pastas em ~/.claude/skills/brasa-bulk-import/ conforme 
especificado na seção 3.2 do relatório. Não preencha conteúdo ainda, só estrutura.

Critério de aceitação:
- Todas as pastas e arquivos vazios existem
- README.md no nível raiz com versão inicial do conteúdo

Quando terminar, liste arquivos criados e me peça confirmação.

## Fase 2 — SKILL.md principal

Implemente o SKILL.md conforme seção 3.5 do relatório, expandindo cada subseção 
de "Commands" para spec completa de inputs, outputs, e comportamento.

Critérios:
- Frontmatter YAML válido com name e description
- Seção "When to use this skill" clara
- Cada comando tem: args, validações, side effects, output esperado
- Seção de guardrails detalhada
- Sentence case em headings
- Sem emoji exceto em exemplos de output

Quando terminar, mostre o SKILL.md completo e me peça revisão.

## Fase 3 — Scripts Python

Implemente os scripts em scripts/ um a um, na ordem:
1. validate_csv.py (adapte de import_conf.md, considerando que vamos passar 
   path como argumento via click)
2. split_csv.py (divide em batches de 250)
3. upload_job.py (cria estrutura S3 + manifest.json)
4. schedule_job.py (cria EventBridge schedule)
5. run_job_now.py (invoca orchestrator Lambda diretamente)
6. status_job.py (lê S3 + SQS para construir status)
7. aggregate_report.py (baixa ZIPs e agrega)
8. cancel_job.py (purge SQS + delete schedule)
9. refresh_token.py (executa docker exec para gerar JWT novo, atualiza env var 
   do Lambda worker via aws lambda update-function-configuration, persiste 
   timestamp em config/last_token_refresh.json)

Critérios para cada script:
- Use click para argumentos
- Inclua --dry-run flag onde fizer sentido
- Output em JSON ou texto formatado, configurável
- Erros como exceptions claras com exit code != 0
- Sem dependências além de: boto3, requests, click, python-dateutil

Atenção especial em refresh_token.py:
- Aceita argumento --target (staging | prod)
- Para staging: docker exec brasa_backend_dev
- Para prod: docker exec brasa_backend_prod (ou container equivalente, 
  conforme config)
- Captura apenas a última linha do output (o token)
- Valida formato do token (deve começar com "eyJ" — assinatura JWT)
- Após atualizar Lambda, grava timestamp ISO em config/last_token_refresh.json:
  {"staging": "2026-05-17T14:32:11-03:00", "prod": null}

Quando terminar cada script, mostre código e me peça confirmação antes do próximo.

## Fase 4 — Lambdas

Implemente as Lambda functions:
1. lambdas/orchestrator/handler.py
2. lambdas/worker/handler.py

Cada uma com:
- requirements.txt minimalista
- deploy.sh que faz pip install, zip, e aws lambda create-function (ou update)
- Tratamento de erro robusto
- Logging em CloudWatch via print() (Lambda padrão)
- Idempotência (mesma mensagem SQS recebida 2x não duplica trabalho — mas dado 
  que o endpoint já é idempotente, basta logar)

Lembre que o worker faz POST multipart/form-data e salva o ZIP de resposta. 
NÃO tenta parsear o ZIP — só armazena no S3. O parsing fica no aggregate_report.py.

Quando terminar, mostre código de ambas e me peça revisão.

## Fase 5 — Infraestrutura

Implemente infra/setup_aws.sh que:
1. Cria bucket S3 com versionamento e lifecycle
2. Cria DLQ e fila principal
3. Cria IAM roles (lê de policies/*.json)
4. Faz deploy dos 2 Lambdas
5. Cria event source mapping SQS → Lambda worker
6. Imprime resumo dos ARNs criados ao final

E os 3 arquivos em policies/:
- lambda_worker_role.json
- orchestrator_role.json
- eventbridge_role.json

Imprima cada arquivo e me peça aprovação ANTES de eu rodar setup_aws.sh.

Importante: setup_aws.sh deve ser idempotente. Se um recurso já existe, ele 
atualiza/skip em vez de falhar.

## Após as 5 fases

Faça um smoke test final:
1. Crie CSV de teste com 10 linhas válidas
2. Execute: prepare → run-now → status → report
3. Mostre output de cada passo
4. Confirme que tudo funcionou

Se algum passo falhar, pare e me peça ajuda para diagnosticar.

---

Antes de começar, confirme que tem:
- AWS CLI configurada (rode: aws sts get-caller-identity)
- Acesso à conta correta da BRASA
- Permissões para criar S3, Lambda, IAM, SQS, EventBridge

Vamos começar pela Fase 1?
```

### 8.2 Comandos auxiliares para Claude Code durante execução

Durante as fases, você (Ricardo) pode pedir verificações como:
- "Mostre o output de `aws s3 ls s3://brasa-migrations/`"
- "Liste as Lambda functions com prefixo brasa-"
- "Mostre o conteúdo atual do manifest.json do job X"

A skill final inclui o comando `list-jobs` que faz isso de forma estruturada.

---

## 9. Riscos, mitigações e decisões

### 9.1 Matriz de riscos

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|-----------|
| JWT expira durante job longo | Baixa | Alto | Token dura 3 dias; `refresh-token` antes de cada janela; guardrail >48h alerta, >60h bloqueia |
| Backend cai durante execução | Baixa | Alto | SQS retém mensagens 4 dias; jobs continuam quando volta |
| CSV mal formatado descoberto no batch 50 | Média | Médio | Validação completa em `prepare`, antes de upload |
| Custo AWS surpreende | Muito baixa | Baixo | Volume cabe folgado em free tier; estimativa: <$1/mês |
| Skill perdida em rotatividade do time | Média | Alto | README.md detalhado + relatório arquivado |
| Conflito de job_id | Baixa | Médio | Guardrail nativo + verificação automática |
| AWS quota limit (Lambda concurrency) | Muito baixa | Médio | Concurrency=1 evita; quota default é 1000 |
| Operador roda PROD por engano | Baixa | Catastrófico | Frase exata + janela horária restrita |
| JWT em env var exposta no Lambda | Baixa | Médio | IAM restritivo ao grupo de operadores |

### 9.2 Decisões arquiteturais defensáveis

**Por que SQS + Lambda em vez de Step Functions:**
SQS+Lambda é o padrão mais simples para "fila de trabalhos com retry". Step Functions é melhor para "workflows com múltiplas etapas dependentes". Aqui cada batch é independente — SQS+Lambda é a escolha certa.

**Por que concurrency=1 padrão:**
Backend Django provavelmente não foi testado para requests paralelas pesadas. Concurrency=1 garante comportamento idêntico ao seu pipeline atual de testes. Pode-se aumentar depois com monitoramento.

**Por que batch_size = 250:**
- Lambda timeout máximo: 15 minutos
- Throughput observado: 0,58 linhas/s
- 250 / 0,58 = ~430s = ~7min
- Folga: 8min para retry/network/etc dentro do mesmo Lambda invocation

**Por que one-time EventBridge Schedule e não cron:**
Cada job é um evento único, agendado para horário específico. Cron seria adequado se houvesse "rodar todo dia às 3h", o que não é o caso.

**Por que NÃO Step Functions para visualização:**
Step Functions tem UI bonita mas adiciona custo e complexidade. SQS console + CloudWatch + a própria skill (comando `status`) dão a mesma observabilidade.

**Por que NÃO Secrets Manager:**
O JWT do BRASA só pode ser gerado via shell no container Django (`docker exec brasa_backend_dev python core/manage.py shell ...`). Não há endpoint HTTP para Lambda buscar dinamicamente. Secrets Manager teria que ser populado **manualmente** pelo operador — o que é exatamente o mesmo trabalho de atualizar a env var do Lambda diretamente, com custo extra de $0.40/mês. Como o token dura 3 dias e a janela de execução é previsível, env var diretamente no Lambda é a escolha mais simples e auditável.

### 9.3 O que NÃO está no escopo desta skill

Para clareza, a skill **NÃO**:
- Implementa o endpoint `/import-master-csv/` (já existe)
- Trata de migração de dados que não sejam CSV de membros
- Faz rollback automático em caso de erro (rollback é decisão humana via snapshot RDS)
- Substitui o fluxo `import_conf.md` para arquivos pequenos (< 500 linhas)
- Gera dashboards visuais (apenas relatórios JSON e CSV)
- Notifica via Slack/email (operador checa via `status` manualmente)

Esses itens são candidatos a iterações futuras se houver demanda.

---

## 10. Apêndices

### Apêndice A — IAM Policies (JSON completo)

**lambda_worker_role.json:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:us-east-1:*:brasa-bulk-import-queue"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::brasa-migrations/jobs/*/inputs/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::brasa-migrations/jobs/*/outputs/*",
        "arn:aws:s3:::brasa-migrations/jobs/*/state/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

> Sem permissão para Secrets Manager — o JWT vem da env var do próprio Lambda, configurada pelo operador via `refresh-token`.

**orchestrator_role.json:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::brasa-migrations/jobs/*/manifest.json"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::brasa-migrations",
      "Condition": {
        "StringLike": {
          "s3:prefix": "jobs/*/inputs/"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": ["sqs:SendMessage"],
      "Resource": "arn:aws:sqs:us-east-1:*:brasa-bulk-import-queue"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

**eventbridge_role.json:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["lambda:InvokeFunction"],
      "Resource": "arn:aws:lambda:us-east-1:*:function:brasa-bulk-import-orchestrator"
    }
  ]
}
```

### Apêndice B — Estimativa de custos AWS

Para 24K registros em DEV + 24K em PROD ao longo de ~3 meses:

| Serviço | Uso estimado | Custo |
|---------|--------------|-------|
| Lambda invocations | ~200 invocations | $0 (free tier: 1M/mês) |
| Lambda compute | ~200 × 7min × 512MB | $0 (free tier: 400K GB-s/mês) |
| SQS messages | ~200 messages | $0 (free tier: 1M/mês) |
| S3 storage | ~5 GB total | <$0.15 |
| S3 requests | ~2K PUT/GET | $0 (free tier suficiente) |
| EventBridge Scheduler | ~20 invocations | <$0.01 |
| CloudWatch Logs | ~1 GB | <$0.50 |
| **Total estimado** | | **< $1/mês** |

### Apêndice C — Glossário

- **Batch:** subdivisão do CSV original em arquivo menor (250 linhas)
- **Job:** uma execução completa da migração de um CSV, identificado por job_id
- **DLQ:** Dead Letter Queue — fila onde mensagens vão após esgotar retries
- **One-time schedule:** EventBridge Scheduler que dispara apenas uma vez
- **Orchestrator:** Lambda que prepara a fila SQS quando um job é disparado
- **Worker:** Lambda que processa cada batch individual
- **Manifest:** arquivo JSON com metadados do job, persistido no S3

### Apêndice D — Fontes consultadas

- AWS Documentation — Lambda + SQS event source mapping
- AWS Documentation — EventBridge Scheduler one-time expressions
- Backend BRASA — endpoint `/import-master-csv/` (existente)
- Pipeline atual — `import_conf.md` (cedido por Ricardo)
- Conversas anteriores sobre arquitetura assíncrona (registradas para o documento de transição)

---

## Próximos passos imediatos para Ricardo

Em ordem cronológica:

1. **Revisar este documento** e marcar dúvidas ou pontos a ajustar
2. **Confirmar conta AWS** está com `aws sts get-caller-identity` retornando dados da BRASA
3. **Abrir Claude Code** em sessão nova com este relatório como contexto
4. **Executar prompt da seção 8.1** em fases, revisando cada uma
5. **Smoke test em staging** com CSV de 10 linhas
6. **Smoke test com 500 linhas** validando pipeline completo
7. **Primeiro lote real em DEV** (3K registros, agendado)
8. **Validação dos dados em DEV** com time de Dados
9. **Apenas após validação completa em DEV:** planejar primeira noite de PROD
10. **Documentar lições aprendidas** após cada lote

Quando terminar a migração completa, este documento + os logs dos jobs viram material para o playbook de tech da BRASA.

---

*Documento preparado por Claude, baseado em conversas com Ricardo Castilho (Gerente Interino de Tecnologia BRASA). Última atualização: 16 de maio de 2026. Versão: 1.1 (revisão após feedback sobre gestão de JWT).*
