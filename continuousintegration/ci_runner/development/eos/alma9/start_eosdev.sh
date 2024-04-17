#!/bin/bash -e

usage() { cat <<EOF 1>&2
Usage: $0 [-p <port>] [-v <volumemount>] <image_tag>
Options:
  <image_tag>       Image to run
  -p                SSH port (need to be opened on firewall)
  -v                Volume mount
EOF
exit 1
}

port=2222
volume_mount="${HOME}/shared:/root/shared:Z"

while getopts "p:v:" o; do
    case "${o}" in
        p)
            port=${OPTARG}
            ;;
        v)
            volumen_mount=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "$1" ]; then
    usage
fi

image_tag=$1
echo "podman run -d -it -p ${port}:22 -v ${volume_mount} ${image_tag}"
container_id=$(podman run -d -it -p ${port}:22 -v ${volume_mount} ${image_tag})
echo "Container ID: ${container_id:0:12}"
echo ""
echo "Container running in detached mode"
echo "To access, add your public key to ${container_id} and ssh to port ${port}"
