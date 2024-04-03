sudo losetup -fP  --direct-io=on -L ./disk
DEVICE=$(sudo losetup -j ./disk | cut -d: -f1)
echo 'export BLOCK='$DEVICE > env/block_device.env
script/build_env.sh