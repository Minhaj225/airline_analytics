#!/usr/bin/env bash
# Explicit environment setup: preserves HDFS data and backs up the one edited file.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
jdk=${1:?Usage: configure_native.sh ABSOLUTE_JDK11_DIRECTORY}
[[ "$jdk" == /* && -x "$jdk/bin/java" ]] || { echo 'Invalid JDK directory' >&2; exit 1; }
version=$("$jdk/bin/java" -version 2>&1)
[[ "$version" == *'version "11.'* ]] || { echo 'JDK 11 required' >&2; exit 1; }
hadoop_home=$(dirname "$(dirname "$(readlink -f "$(command -v hadoop)")")")
spark_home=$(dirname "$(dirname "$(readlink -f "$(command -v spark-shell)")")")
spark_java=$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")
evidence="$root/artifacts/setup-completion"
mkdir -p "$evidence"
python3 - "$root" "$jdk" "$hadoop_home" "$spark_home" "$spark_java" <<'PY'
import pathlib, re, shlex, sys
root, jdk, hadoop, spark, java = sys.argv[1:]
root = pathlib.Path(root)
config = root / 'config/project.env'
if config.exists():
    raise SystemExit('Project config already exists; inspect it before reconfiguring')
path = pathlib.Path(hadoop) / 'etc/hadoop/hadoop-env.sh'
text = path.read_text()
backup = root / 'artifacts/setup-completion/hadoop-env.sh.before'
if not backup.exists():
    backup.write_text(text)
assignments = list(re.finditer(r'(?m)^export JAVA_HOME=.*$', text))
if len(assignments) != 1:
    raise SystemExit('Expected one active JAVA_HOME assignment; inspect configuration')
match = assignments[0]
updated = text[:match.start()] + 'export JAVA_HOME=' + shlex.quote(jdk) + text[match.end():]
path.write_text(updated)
values = dict(JAVA_HOME=java, HADOOP_JAVA_HOME=jdk, HADOOP_HOME=hadoop,
              SPARK_HOME=spark, HADOOP_CONF_DIR=str(path.parent))
config.write_text('\n'.join(k + '=' + shlex.quote(v) for k, v in values.items()) + '\n' +
                  'MODE=fixture\nSTORAGE=hdfs\nSPARK_MASTER="local[2]"\nDRIVER_MEMORY=2g\n')
print('Configured separate Hadoop/Spark JVMs; original Hadoop environment backed up. No storage changes.')
PY
