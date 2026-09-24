#!/usr/bin/env bash
# Runner de testes SQL contra o ambiente sybase-fdw-test.
#
# Cada arquivo em fixtures/*.sql é executado isoladamente via psql. O runner
# captura:
#   - exit code do psql (0 = ok, !=0 = falhou)
#   - se houve crash do backend (sinal SIGSEGV deixa rastro nos logs)
#   - tempo de execução
#   - output completo em results/<name>.{out,err}
#
# Uso:
#   ./scripts/run-tests.sh                       # roda todos
#   ./scripts/run-tests.sh fixtures/bug1_*.sql   # roda arquivos específicos
#
# Cada fixture inicia com um header `-- @expect:` que define o resultado:
#   -- @expect: ok                     (psql exit 0, sem crash)
#   -- @expect: error                  (qualquer erro do psql)
#   -- @expect: error <SQLSTATE>       (erro contendo o SQLSTATE específico)
#   -- @expect: crash                  (backend morre por sinal)
#   -- @expect: ok-or-error <SQLSTATE> (ok ou erro do SQLSTATE; sem crash)
set -uo pipefail

# Colors
RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'

CONTAINER=sybase-fdw-test-postgres
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
FIXTURES_DIR=$(dirname "$SCRIPT_DIR")/fixtures
RESULTS_DIR=$(dirname "$SCRIPT_DIR")/results
mkdir -p "$RESULTS_DIR"

# Verifica que o container está de pé.
if ! docker exec "$CONTAINER" pg_isready -h localhost -U postgres >/dev/null 2>&1; then
    echo "${RED}ERR${RESET} Postgres não está pronto. Rode 'make up' primeiro."
    exit 2
fi

# Snapshot da contagem de linhas do log do container ANTES do teste, para
# detectar crashes a partir desse offset.
LOG_OFFSET=$(docker logs "$CONTAINER" 2>&1 | wc -l | tr -d ' ')

# Files a rodar
if [ "$#" -gt 0 ]; then
    FILES=("$@")
else
    FILES=("$FIXTURES_DIR"/*.sql)
fi

declare -i PASSED=0 FAILED=0 SEGFAULT=0
declare -a FAILED_NAMES

for raw_f in "${FILES[@]}"; do
    f=$raw_f
    [ ! -f "$f" ] && [ -f "$FIXTURES_DIR/$f" ] && f="$FIXTURES_DIR/$f"
    [ ! -f "$f" ] && { echo "${YELLOW}SKIP${RESET} $raw_f (não encontrado)"; continue; }

    name=$(basename "$f" .sql)
    out_file="$RESULTS_DIR/${name}.out"
    err_file="$RESULTS_DIR/${name}.err"

    expect=$(grep -m1 '^-- @expect:' "$f" 2>/dev/null | sed 's/^-- @expect:[[:space:]]*//' || echo ok)
    expect=${expect:-ok}

    desc=$(grep -m1 '^-- @desc:' "$f" 2>/dev/null | sed 's/^-- @desc:[[:space:]]*//' || echo "")

    printf '  %-50s [%s] ' "$name" "$expect"

    start_ts=$(date +%s%N)
    # Capturamos stdout/stderr separadamente para diferenciar resultados de erros.
    # ON_ERROR_STOP=1 faz o psql sair no primeiro erro.
    # docker exec precisa de -i para encaminhar stdin do host (o arquivo SQL).
    docker exec -i "$CONTAINER" psql -h localhost -U postgres -X \
        -v ON_ERROR_STOP=1 -P pager=off < "$f" \
        > "$out_file" 2> "$err_file"
    ec=$?
    end_ts=$(date +%s%N)
    elapsed_ms=$(( (end_ts - start_ts) / 1000000 ))

    # Detecta crash do backend (sinal 11) consultando os logs novos desde
    # o offset anterior. BSD/macOS tail aceita "+N" mas o awk é mais portável.
    crashed=no
    if docker logs "$CONTAINER" 2>&1 | awk -v off="$LOG_OFFSET" 'NR>off' | \
       grep -qE 'terminated by signal 11|server process .* was terminated by signal'; then
        crashed=yes
    fi
    LOG_OFFSET=$(docker logs "$CONTAINER" 2>&1 | wc -l | tr -d ' ')

    # Classifica resultado.
    result=fail
    case "$expect" in
        ok)
            if [ "$ec" -eq 0 ] && [ "$crashed" = no ]; then result=pass; fi
            ;;
        crash)
            if [ "$crashed" = yes ]; then result=pass; fi
            ;;
        error)
            if [ "$ec" -ne 0 ] && [ "$crashed" = no ]; then result=pass; fi
            ;;
        error\ *)
            sqlstate=${expect#error }
            if grep -q "SQLSTATE.*$sqlstate\|XX000\|$sqlstate" "$err_file" && [ "$crashed" = no ]; then
                result=pass
            fi
            ;;
        ok-or-error\ *)
            if [ "$ec" -eq 0 ] || grep -q "ERROR" "$err_file"; then
                if [ "$crashed" = no ]; then result=pass; fi
            fi
            ;;
    esac

    if [ "$result" = pass ]; then
        printf '%sPASS%s (%dms)\n' "$GREEN" "$RESET" "$elapsed_ms"
        PASSED=$((PASSED+1))
    else
        printf '%sFAIL%s (ec=%d crashed=%s %dms)\n' "$RED" "$RESET" "$ec" "$crashed" "$elapsed_ms"
        [ -n "$desc" ] && echo "      $desc"
        echo "      stderr (truncated):"
        head -5 "$err_file" 2>/dev/null | sed 's/^/        /'
        FAILED=$((FAILED+1))
        FAILED_NAMES+=("$name")
        [ "$crashed" = yes ] && SEGFAULT=$((SEGFAULT+1))
    fi
done

echo
echo "==> Resultado: ${GREEN}${PASSED} passed${RESET}, ${RED}${FAILED} failed${RESET} (${SEGFAULT} crash)"
[ $FAILED -gt 0 ] && {
    echo "    Falhas:"
    for n in "${FAILED_NAMES[@]}"; do
        echo "      - $n  (ver results/$n.{out,err})"
    done
    exit 1
}
exit 0
