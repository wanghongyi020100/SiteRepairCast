#!/usr/bin/env bash

set -euo pipefail

usage()
{
    echo "用法: $0 [输出文件]"
    echo "示例: $0 site_all_code.txt"
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]
then
    usage
    exit 0
fi

if ! project_root=$(git rev-parse --show-toplevel 2>/dev/null)
then
    echo "错误：当前目录不在 Git 仓库中。请先进入 SiteRepairCast 仓库根目录。" >&2
    exit 1
fi

output_arg=${1:-site_all_code.txt}
if [[ $output_arg = /* ]]
then
    output_file=$output_arg
else
    output_file="$PWD/$output_arg"
fi

mkdir -p "$(dirname "$output_file")"
output_file=$(realpath -m "$output_file")
temporary_file=$(mktemp "${output_file}.tmp.XXXXXX")
trap 'rm -f "$temporary_file"' EXIT

cd "$project_root"

language_of()
{
    case "$1" in
        *.cpp|*.cc|*.cxx|*.hpp|*.hh|*.h) echo "C/C++" ;;
        *.py) echo "Python" ;;
        *.sh) echo "Shell" ;;
        *.cmake|CMakeLists.txt) echo "CMake" ;;
        *.json) echo "JSON" ;;
        *.md) echo "Markdown" ;;
        *.txt) echo "Text" ;;
        *.yml|*.yaml) echo "YAML" ;;
        .gitignore|.gitattributes) echo "Git configuration" ;;
        *) echo "Text/Other" ;;
    esac
}

skip_sensitive()
{
    case "$1" in
        .env|*/.env|*.pem|*.key|*.p12|*.pfx|id_rsa|*/id_rsa)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

{
    echo "SiteRepairCast 本地源码汇总"
    echo "生成时间: $(date '+%Y-%m-%d %H:%M:%S %z')"
    echo "仓库根目录: $project_root"
    echo "Git commit: $(git rev-parse HEAD)"
    echo
    echo "==================== DIRECTORY TREE ===================="

    if command -v tree >/dev/null 2>&1
    then
        tree -a -I '.git|build|build-*|artifacts|__pycache__|*.pyc|site_all_code.txt'
    else
        find . \
            -path './.git' -prune -o \
            -path './build' -prune -o \
            -path './build-*' -prune -o \
            -path './artifacts' -prune -o \
            -path '*/__pycache__' -prune -o \
            -type f -print | sort
    fi

    echo
    echo "==================== FILE INDEX ===================="
    echo "来源：git 已跟踪文件 + 未被 .gitignore 忽略的未跟踪文件"
    echo

    file_count=0
    skipped_count=0

    while IFS= read -r -d '' path
    do
        [[ -f $path ]] || continue

        absolute_path=$(realpath -m "$project_root/$path")
        if [[ $absolute_path == "$output_file" || $absolute_path == "$temporary_file" ]]
        then
            continue
        fi

        if skip_sensitive "$path"
        then
            echo "[跳过敏感文件] $path"
            skipped_count=$((skipped_count+1))
            continue
        fi

        if [[ -s $path ]] && ! grep -Iq . "$path"
        then
            echo "[跳过二进制文件] $path"
            skipped_count=$((skipped_count+1))
            continue
        fi

        file_count=$((file_count+1))
        echo
        echo "===== BEGIN FILE: $path ====="
        echo "类型: $(language_of "$path")"
        echo "大小: $(wc -c < "$path") bytes"
        echo "----- CONTENT -----"
        sed -n '1,$p' "$path"
        echo
        echo "===== END FILE: $path ====="
    done < <(git ls-files --cached --others --exclude-standard -z)

    echo
    echo "==================== SUMMARY ===================="
    echo "写入文本文件数: $file_count"
    echo "跳过文件数: $skipped_count"
} > "$temporary_file"

mv "$temporary_file" "$output_file"
trap - EXIT

echo "完成：$output_file"
echo "大小：$(wc -c < "$output_file") bytes"

