"""
userstorage configuration
"""

from userstorage import File, Mount, LoopDevice

GiB = 1024**3

BASE_DIR = "/var/tmp/imageio-storage"

BACKENDS = {

    # This storage is generally not interesting for testing, but since it does
    # not support fallocate(), it is a good way to test the fallback to manualy
    # writing zeroes.
    "file-512-ext2": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-512-ext2",
                size=GiB,
                sector_size=512,
            ),
            fstype="ext2",
        )
    ),

    "file-512-ext4": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-512-ext4",
                size=GiB,
                sector_size=512,
            ),
            fstype="ext4",
        )
    ),

    "file-512-xfs": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-512-xfs",
                size=GiB,
                sector_size=512,
                # Fails to mount on Jenkins when running on slave with kernel
                # 3.10. Let's make it optional so we can test other storage.
                required=False,
            ),
            fstype="xfs",
        )
    ),

    "file-4k-ext2": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-4k-ext2",
                size=GiB,
                sector_size=4096,
                required=False,
            ),
            fstype="ext2",
        )
    ),

    "file-4k-ext4": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-4k-ext4",
                size=GiB,
                sector_size=4096,
                required=False,
            ),
            fstype="ext4",
        )
    ),

    "file-4k-xfs": File(
        Mount(
            LoopDevice(
                base_dir=BASE_DIR,
                name="file-4k-xfs",
                size=GiB,
                sector_size=4096,
                required=False,
            ),
            fstype="xfs",
        )
    ),

}
