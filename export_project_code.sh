#!/usr/bin/env bash

set -euo pipefail

usage()
{
    echo "用法: $0 [输出文件]"
    echo "示例: $0 site_code.txt"
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]
then
    usage
    exit 0
fi

# 必须在 Git 仓库内运行
if ! project_root=$(git rev-parse --show-toplevel 2>/dev/null)
then
    echo "错误：当前目录不在 Git 仓库中。" >&2
    exit 1
fi

output_arg=${1:-site_code.txt}

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

# 判断是不是需要导出的代码文件
is_code_file()
{
    local path=$1
    local name=${path##*/}

    case "$name" in
        CMakeLists.txt)
            return 0
            ;;
    esac

    case "$path" in
        *.cpp|*.cc|*.cxx|*.c|\
        *.hpp|*.hh|*.hxx|*.h|\
        *.cmake)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 排除测试、benchmark、结果、构建文件等
should_skip()
{
    local path=$1
    local lower

    lower=$(printf '%s' "$path" | tr '[:upper:]' '[:lower:]')

    # -------------------------
    # 目录排除
    # -------------------------
    case "/$lower/" in
        */.git/*|\
        */build/*|\
        */build-*/*|\
        */cmake-build-*/*|\
        */out/*|\
        */dist/*|\
        */artifacts/*|\
        */artifact/*|\
        */result/*|\
        */results/*|\
        */output/*|\
        */outputs/*|\
        */log/*|\
        */logs/*|\
        */tmp/*|\
        */temp/*|\
        */__pycache__/*|\
        */test/*|\
        */tests/*|\
        */testing/*|\
        */benchmark/*|\
        */benchmarks/*|\
        */bench/*|\
        */benches/*|\
        */perf/*)
            return 0
            ;;
    esac

    local name=${lower##*/}

    # -------------------------
    # 测试源码排除
    # -------------------------
    case "$name" in
        test_*.cpp|test_*.cc|test_*.cxx|test_*.c|\
        test_*.hpp|test_*.hh|test_*.h|\
        *_test.cpp|*_test.cc|*_test.cxx|*_test.c|\
        *_test.hpp|*_test.hh|*_test.h|\
        *_tests.cpp|*_tests.cc|*_tests.cxx|*_tests.c|\
        *_tests.hpp|*_tests.hh|*_tests.h|\
        benchmark_*.cpp|benchmark_*.cc|benchmark_*.cxx|\
        *_benchmark.cpp|*_benchmark.cc|*_benchmark.cxx|\
        bench_*.cpp|bench_*.cc|bench_*.cxx|\
        *_bench.cpp|*_bench.cc|*_bench.cxx)
            return 0
            ;;
    esac

    # CMake 自动生成文件
    case "$lower" in
        */cmakefiles/*|\
        */cmakecache.txt|\
        */cmake_install.cmake)
            return 0
            ;;
    esac

    return 1
}

language_of()
{
    case "$1" in
        *.cpp|*.cc|*.cxx|*.c) echo "C/C++ Source" ;;
        *.hpp|*.hh|*.hxx|*.h) echo "C/C++ Header" ;;
        *.cmake|*/CMakeLists.txt|CMakeLists.txt) echo "CMake" ;;
        *) echo "Code" ;;
    esac
}

{
    echo "SiteRepairCast 源代码汇总"
    echo "Git commit: $(git rev-parse HEAD)"
    echo

    file_count=0

    while IFS= read -r -d '' path
    do
        [[ -f $path ]] || continue

        # 只保留代码文件
        if ! is_code_file "$path"
        then
            continue
        fi

        # 排除测试、benchmark、结果等
        if should_skip "$path"
        then
            continue
        fi

        absolute_path=$(realpath -m "$project_root/$path")

        # 防止把当前输出文件再次复制进去
        if [[ $absolute_path == "$output_file" ||
              $absolute_path == "$temporary_file" ]]
        then
            continue
        fi

        # 跳过二进制文件
        if [[ -s $path ]] && ! grep -Iq . "$path"
        then
            continue
        fi

        file_count=$((file_count+1))

        echo
        echo "============================================================"
        echo "FILE: $path"
        echo "TYPE: $(language_of "$path")"
        echo "============================================================"
        cat "$path"
        echo

    done < <(
        git ls-files \
            --cached \
            --others \
            --exclude-standard \
            -z |
        sort -z
    )

    echo
    echo "============================================================"
    echo "共导出代码文件: $file_count"
    echo "============================================================"

} > "$temporary_file"

mv "$temporary_file" "$output_file"

trap - EXIT

echo "完成：$output_file"
echo "代码文件数：$file_count"
echo "文件大小：$(wc -c < "$output_file") bytes"