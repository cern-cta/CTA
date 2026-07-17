# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

_ctadev() {
    local -r project_root=$(git rev-parse --show-toplevel)
    local cur prev command
    COMPREPLY=()

    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    command="${COMP_WORDS[1]}"

    local global_opts="
        --container-runtime
        --platform
        --scheduler-type
        --disable-oracle-support
        --use-public-repos
    "

    local build_opts="
        --reset
        --clean-build-dir
        --clean-build-dirs
        --disable-ccache
        --skip-cmake
        --skip-debug-packages
        --enable-unit-tests
        --force-install
        --enable-address-sanitizer
        --build-generator
        --cmake-build-type
    "

    local image_opts="
        --skip-image-cleanup
        --enable-debug-image
    "

    local deploy_opts="
        --with-dcache
        --local-telemetry
        --publish-telemetry
        --eos-image-repository
        --eos-image-tag
        --cta-image-tag
        --catalogue-config
        --scheduler-config
        --cta-config
        --eos-config
        --spawn-options
        --deploy-namespace
    "

    #
    # Complete values for options that expect one.
    #
    case "$prev" in
        --container-runtime)
            COMPREPLY=( $(compgen -W "docker podman" -- "$cur") )
            return
            ;;
        --scheduler-type)
            COMPREPLY=( $(compgen -W "objectstore pgsched" -- "$cur") )
            return
            ;;
        --cmake-build-type)
            COMPREPLY=( $(compgen -W \
                "Release Debug RelWithDebInfo MinSizeRel" \
                -- "$cur") )
            return
            ;;
        --build-generator)
            COMPREPLY=( $(compgen -W \
                "Ninja Unix\\ Makefiles" \
                -- "$cur") )
            return
            ;;
        --platform)
            COMPREPLY=( $(compgen -W "el9" -- "$cur") )
            return
            ;;
        --deploy-namespace)
            return
            ;;
        --cta-image-tag|--eos-image-tag|--eos-image-repository|--spawn-options)
            return
            ;;
        --catalogue-config|--scheduler-config|--cta-config|--eos-config)
            COMPREPLY=( $(compgen -f -- "$cur") )
            return
            ;;
    esac

    #
    # Complete command.
    #
    if [[ $COMP_CWORD -eq 1 ]]; then
        COMPREPLY=( $(compgen -W \
            "build images deploy test up all install help" \
            -- "$cur") )
        return
    fi

    #
    # Complete test name.
    #
    if [[ "$command" == "test" || "$command" == "all" ]]; then
        local i
        local seen_test=false

        for ((i=2; i<COMP_CWORD; ++i)); do
            if [[ "${COMP_WORDS[i]}" != -* ]]; then
                seen_test=true
                break
            fi
        done

        if [[ "$seen_test" == false && "$cur" != -* ]]; then
            local tests=""
            if [[ -d "${project_root}/ci/system_tests/tests" ]]; then
                tests=$(for f in "${project_root}"/ci/system_tests/tests/*_test.py; do
                    basename "$f" _test.py
                done)
            fi

            COMPREPLY=( $(compgen -W "$tests" -- "$cur") )
            return
        fi
    fi

    #
    # Complete options.
    #
    case "$command" in
        build)
            COMPREPLY=( $(compgen -W \
                "$global_opts $build_opts" \
                -- "$cur") )
            ;;
        images)
            COMPREPLY=( $(compgen -W \
                "$global_opts $image_opts" \
                -- "$cur") )
            ;;
        deploy)
            COMPREPLY=( $(compgen -W \
                "$global_opts $deploy_opts" \
                -- "$cur") )
            ;;
        test)
            COMPREPLY=( $(compgen -W \
                "$global_opts --deploy-namespace" \
                -- "$cur") )
            ;;
        up)
            COMPREPLY=( $(compgen -W \
                "$global_opts $build_opts $image_opts $deploy_opts" \
                -- "$cur") )
            ;;
        all)
            COMPREPLY=( $(compgen -W \
                "$global_opts $build_opts $image_opts $deploy_opts" \
                -- "$cur") )
            ;;
        install|help)
            COMPREPLY=( $(compgen -W \
                "$global_opts" \
                -- "$cur") )
            ;;
    esac
}

complete -F _ctadev cta-dev
