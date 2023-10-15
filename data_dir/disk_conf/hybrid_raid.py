#!/usr/bin/env python3
"""
This script ensures that the system is in a state in which scylla can successfully start.
Such logic includes:
  * Verifying the filesystem on top of NVMe RAID and/or a single pd-ssd
  * Re-configuring mounts to match the state of the system
  * Configuring scylla to replace itself if this node lost its data
"""
import argparse
import datetime
import json
import logging
import os
import re
import subprocess
import time
from typing import List, NamedTuple, Optional
# pylint: disable=all

CIRCUIT_BREAK_FILE = '/scylla_circuit_break'


class CircuitBreakError(Exception):
    pass


class IOPerf(NamedTuple):
    read_iops: int
    write_iops: int
    read_bandwidth: int
    write_bandwidth: int

    def scale(self, value: float) -> 'IOPerf':
        return IOPerf(
            read_iops=int(self.read_iops * value),
            write_iops=int(self.write_iops * value),
            read_bandwidth=int(self.read_bandwidth * value),
            write_bandwidth=int(self.write_bandwidth * value),
        )


def _ioperf_gcp_ssd(size: int) -> IOPerf:
    # https://cloud.google.com/compute/docs/disks/performance#ssd_persistent_disk
    return IOPerf(
        read_iops=min(int(size / (1024 * 1024 * 1024)) * 30, 75000),
        write_iops=min(int(size / (1024 * 1024 * 1024)) * 30, 25000),
        read_bandwidth=min(int(size / 2048), 1200 * 1024 * 1024),
        write_bandwidth=min(int(size / 2048), 800 * 1024 * 1024),
    )


def _ioperf_gcp_nvme(count: int) -> IOPerf:
    # https://cloud.google.com/compute/docs/disks/local-ssd#nvme
    if count <= 15:
        factor = min(count, 4)  # 4-8 disks all perform the same
        ioperf = IOPerf(170000 * factor, 90000 * factor, 660 * factor, 350 * factor)
    elif count < 23:
        ioperf = IOPerf(1600000, 800000, 6240, 3120)
    else:
        ioperf = IOPerf(2400000, 1200000, 9360, 4680)
    return IOPerf(
        ioperf.read_iops, ioperf.write_iops, ioperf.read_bandwidth * 1024 * 1024, ioperf.write_bandwidth * 1024 * 1024,
    )


def get_ioperf(nvme_dev: Optional[str], ssd_dev: Optional[str]) -> IOPerf:
    # the nvmes
    nvme_ioperf = None
    if nvme_dev is not None:
        nvme_dev = nvme_dev.split('/')[-1]
        with open(f'/sys/block/{nvme_dev}/md/raid_disks', 'r') as handle:
            disks = int(handle.read().strip())
        # scale down the iops to account for possible raid5 (1 disk)
        nvme_ioperf = _ioperf_gcp_nvme(disks).scale(max(0.5, (disks - 1) / disks))

    # the ssd
    ssd_ioperf = None
    if ssd_dev is not None:
        ssd_dev = ssd_dev.split('/')[-1]
        with open(f'/sys/block/{ssd_dev}/size', 'r') as handle:
            size = int(handle.read().strip()) * 512
        # scale down the iops so that the pd-ssds don't hit their limit and start throttling
        ssd_ioperf = _ioperf_gcp_ssd(size).scale(0.9)

    # combine - if both disks are used, read at nvme specs and write at ssd specs
    if nvme_ioperf and ssd_ioperf:
        return IOPerf(
            read_iops=nvme_ioperf.read_iops,
            write_iops=ssd_ioperf.write_iops,
            read_bandwidth=nvme_ioperf.read_bandwidth,
            write_bandwidth=ssd_ioperf.write_bandwidth,
        )
    if nvme_ioperf:
        return nvme_ioperf
    assert ssd_ioperf
    return ssd_ioperf


def update_io_properties(ioperf: IOPerf, duplex: bool) -> None:
    logging.info('Updating io_properties.yaml (%s)', ioperf)
    with open('/etc/scylla.d/io_properties.yaml', 'w', encoding="utf-8") as handle:
        handle.write('disks:\n  - mountpoint: /var/lib/scylla\n')
        handle.write(f'    read_iops: {ioperf.read_iops}\n')
        handle.write(f'    read_bandwidth: {ioperf.read_bandwidth}\n')
        handle.write(f'    write_iops: {ioperf.write_iops}\n')
        handle.write(f'    write_bandwidth: {ioperf.write_bandwidth}\n')
        if duplex:
            handle.write('    duplex: true\n')


def run(command: List[str], as_sudo: bool = True) -> str:
    if as_sudo:
        command.insert(0, "sudo")
    proc = subprocess.run(command, stdout=subprocess.PIPE, check=True)
    return proc.stdout.decode('utf-8')


def run_shell(command: str) -> str:
    logging.info(f'RUNNING CMD: {command}')
    proc = subprocess.run(command, stdout=subprocess.PIPE, shell=True)
    return proc.stdout.decode('utf-8')


def replace_line(filename: str, regex: str, replacement: Optional[str]) -> None:
    """Remove all lines matching [regex] from [filename] and append [replacement]"""
    with open(filename, 'r') as handle:
        lines_in = handle.read().splitlines()
    lines_out = [l for l in lines_in if not re.search(regex, l)]
    if replacement is not None:
        lines_out.append(replacement)
    with open(filename, 'w') as handle:
        for line in lines_out:
            handle.write(f'{line}\n')


def md_device(level: Optional[str] = None, device_member: Optional[str] = None) -> Optional[List[str]]:
    """Get the md device with the given raid level (or an inactive device if level is None)"""
    with open('/proc/mdstat', 'r', encoding="utf-8") as handle:
        data = handle.read()
    if device_member is not None:
        device_member_component = device_member.split('/')[-1]
    for line in data.splitlines():
        elements = line.split()
        # do device filtering
        if device_member is not None:
            if len(list(filter(lambda x: x.startswith(device_member_component), elements))) != 1:
                continue
        if level is not None and 'active' in elements and f'raid{level}' in elements:
            return elements
        if level is None and 'inactive' in elements:
            return elements
    return None


def md_device_info(element: str) -> dict:
    """Process the device strings that are generated for each mdset from: /proc/mdstat"""
    device, number = re.match(r'^([^\[]+)\[(\d+)\]', element).groups()
    return {'device': device, 'number': number, 'flags': re.findall(r'\(([^)]+)\)', element), 'state': None}


def md_device_details(level: Optional[str] = None, device: Optional[str] = None) -> Optional[dict]:
    """Get the md device with the given raid level (or an inactive device if level is None)"""
    with open('/proc/mdstat', 'r', encoding="utf-8") as handle:
        data = handle.read()

    personalities = None
    mdset = None
    for line in data.split('\n'):
        elements = line.split()
        if len(elements) >= 1 and elements[0] == 'Personalities':
            # slurp in the personalities
            personalities = list(map(lambda x: x[1: len(x) - 1], elements[2:]))

        if len(elements) >= 4 and elements[1] == ':' and elements[2] in ('active', 'inactive'):
            # filter device name
            if device is not None and elements[0] != device:
                continue

            if elements[2] == 'active':
                # readonly?
                if elements[3][0] == '(' and elements[3][-1] == ')':
                    # auto-read-only is also a state, but just interested in the read-only status, not why
                    if 'read-only' in elements[3]:
                        element_readonly = True
                    del elements[3]
                else:
                    element_readonly = False

                # sanity
                element_level = elements[3]
                if element_level not in personalities:
                    continue

                # filter raid level
                if level is not None and element_level != level:
                    continue

                element_active = True
                element_device_start_position = 4
            else:
                # inactive won't have raid level
                if level is not None:
                    continue
                element_active = False
                element_device_start_position = 3
                element_level = None

            # create the mdset info dict
            raid_devices = list(map(md_device_info, elements[element_device_start_position:]))
            for device in raid_devices:
                member_device = device['device']
                with open(f'/sys/block/{elements[0]}/md/dev-{member_device}/state') as device_state_fp:
                    device['state'] = device_state_fp.readline().rstrip().split(',')
            mdset = {
                'device': elements[0],
                'active': element_active,
                'level': element_level,
                'members': raid_devices,
                'state': 'idle',
                'read_only': element_readonly,
                # active mdraid will have these details on the next line of output #
                'members_total': 0 if not element_active else None,
                'members_online': 0 if not element_active else None,
            }

        elif mdset and mdset['active'] and len(elements) >= 6 and elements[1] == 'blocks' and elements[2] == 'super':
            # get the device status from the [3/6] [__UUU_] string at the end #
            if mdset['level'] != 'raid0':
                status = re.match(r'^\[(\d+)/(\d+)\]$', elements[-2])
                mdset['members_total'] = int(status.group(1))
                mdset['members_online'] = int(status.group(2))
                mdset['members_offline'] = mdset['members_total'] - mdset['members_online']

        elif (
                mdset
                and mdset['active']
                and len(elements) >= 7
                and elements[-2].startswith('finish=')
                and elements[-1].startswith('speed=')
        ):
            # operation in progress #
            mdset['state'] = elements[1]
            progress = re.match(r'^\((\d+)/(\d+)\)$', elements[4])
            mdset['operation'] = {
                'position': int(progress.group(1)),
                'total': int(progress.group(2)),
                'finish': elements[-2][7:],
                'speed': elements[-1][7:],
            }

        elif len(elements) == 0:
            # hit the space between mdsets, if the specified mdset was populated, good to go #
            if mdset:
                return mdset
            mdset = None

    return None


def get_nvmes() -> List[str]:
    if os.getenv('PREFLIGHT_DEVICES_NVME') is not None:
        nvmes = os.getenv('PREFLIGHT_DEVICES_NVME').split(',')
    else:
        blockdevices = json.loads(run(['lsblk', '-dJ']))['blockdevices']
        nvmes = [d['name'] for d in blockdevices if d['name'].startswith('nvme')]
    return sorted([f'/dev/{n}' for n in nvmes])


def ensure_device_striped(nvmes: List[str], raid_level: str) -> str:
    """Ensure /dev/md? exists as a raid array of nvmes and return its name"""
    # if there are fewer than 3 disks, we can't do anything other than raid0
    level = '0' if len(nvmes) < 3 else raid_level
    mdev = md_device(level) or md_device('0')  # old servers may have raid0
    flags = [] if level == '0' else ['-binternal']
    if mdev is None:
        logging.info(f'Creating md device in raid{level} using {nvmes}')
        command = (
            ['mdadm', '--create', '/dev/md0', f'--level={level}', '-c1024', f'--raid-devices={len(nvmes)}']
            + flags
            + nvmes
        )
        run(command)
        mdev = md_device(level)
    assert mdev is not None  # we just created a device (plz)

    if raid_level == '5':
        # speed up syncing for NVMes (they're fast)
        # 3GBs was arbitrarily but intentionally much higher than 24x NVMe (~1.2GBs)
        with open(f'/sys/block/{mdev[0]}/md/sync_speed_max', 'w') as handle:
            handle.write('3000000')
        with open(f'/sys/block/{mdev[0]}/md/group_thread_cnt', 'w') as handle:
            handle.write('4')  # best "bang-for-buck" in (casual) testing

    return f'/dev/{mdev[0]}'


def apply_striped_speed_limit(device: str, raid_level: str) -> None:
    """During normal operation we don't want the RAID sync to sefverely interfere with the service"""
    mdev = device.split('/')[-1]
    if raid_level == '5':
        # speed up syncing for NVMes (they're fast)
        with open(f'/sys/block/{mdev}/md/sync_speed_max', 'w') as handle:
            handle.write('350000')  # https://cloud.google.com/compute/docs/disks/local-ssd#performance
        if os.path.exists(f'/sys/block/{mdev}/md/group_thread_cnt'):
            with open(f'/sys/block/{mdev}/md/group_thread_cnt', 'w') as handle:
                handle.write('4')  # best "bang-for-buck" in (casual) testing


def ensure_device_mirrored(nvme: str, ssd: str, ignore_array_state: bool = False) -> str:
    """Ensure /dev/md? exists as a raid1 array of nvme and ssd and return its name"""
    # Look for existing RAID1 and ensure it's active
    mdev = md_device('1')
    if ignore_array_state:
        # this option only allows ignoring the state of RAID1, ie don't fix it by adding the NVMe RAID0 back in #
        if not mdev:
            raise Exception(f'ignore_array_state requested but no array found; cannoty')
        logging.info(f'Ignoring state of RAID1 array {mdev[0]}')
        return f'/dev/{mdev[0]}'
    forced_activation = False
    if not mdev:
        # the RAID may exist as "inactive" (if the nvmes came up blank, ie fresh NVMes without RAID1 superblock)
        mdev = md_device(device_member=ssd)
        if mdev:
            # RAID1 exists but is inactive due to one device missing RAID1 superblock
            # Force RAID1 to activate into degraded state
            logging.info(f'Activating the degraded array {mdev[0]}')
            run(['mdadm', '--run', f'/dev/{mdev[0]}'])
            forced_activation = True
            time.sleep(1)  # yield long enough for the array to propagate

    raid1_details = md_device_details('raid1')
    if raid1_details:
        # RAID1 does exist, check if it's in a degraded state
        if raid1_details['members_offline'] > 0:
            mdraid1_device_pdssd = list(filter(lambda x: x['device'] == ssd.split('/')[-1], raid1_details['members']))

            # Abort starting Scylla if the RAID1 is active with only the NVMe devices
            if len(mdraid1_device_pdssd) == 0:
                raise CircuitBreakError(
                    'The RAID1 is not online with the PD-SSD; circuit-breaking the scylla-server.service startup. '
                    'Wait for PD service to be come healthy, then power-cycle VM'
                )
            elif 'faulty' in mdraid1_device_pdssd[0]['state']:
                raise CircuitBreakError(
                    'The RAID1 is active, but degraded with PD-SSD marked as faulty. Wait until the PD service is back and healthy, and then power cycle the instance.'
                )
            # Check if PD is ever in a "spare" sate; if so, we want to short-circuit as PD should never
            # be used as a "spare" mirror.
            mdraid1_device_nvme = list(filter(lambda x: x['device'] != ssd.split('/')[-1], raid1_details['members']))
            if 'spare' in mdraid1_device_pdssd[0]['state']:
                raid1_device = raid1_details['device']
                device_pdssd = mdraid1_device_pdssd[0]['device']
                device_nvme = mdraid1_device_nvme[0]['device'] if len(mdraid1_device_nvme) != 0 else None
                if raid1_details['state'] in ('recovery', 'resync'):
                    write_circuit_break(f'mdraid1 /dev/{raid1_device} wrote from {device_nvme} to {device_pdssd}')
                    raise CircuitBreakError(
                        'The RAID1 is online but resyncing from the NVMe; setting a persistent circuit-break flag file and aborting the scylla-server.service startup to prevent data loss!'
                    )
                raise CircuitBreakError(
                    'The RAID1 is in a bad state -- the PD-SSD is presently in a spare state with the NVMe offline; circuit-breaking the scylla-server.service startup to prevent data loss!'
                )

            # Check if NVMe is not a member of RAID1
            device_nvme = nvme.split('/')[-1]
            if len(mdraid1_device_nvme) == 0:
                # There are 2 cases where the RAID1 can be degraded with the NVMes not a member of the RAID1:
                # 1. The NVMe device was previously marked as faulty by the RAID1. We want to short-circuit and instruct
                #    operator to power-cycle the VM to get fresh NVMes.
                #
                # 2. We have "fresh NVMes" and we've "activated a degraded RAID1" in line 357. In this case
                # we add the fresh NVMes to the RAID1, triggering a recovery.

                # Case #1:
                # NVMe marked faulty in RAID1, short-circuit and tell operator to power-cycle
                if not forced_activation:
                    raise CircuitBreakError(
                        'The RAID1 is active, but degraded due to NVMe being previously marked as faulty. Power-cycle the VM to get fresh NVMes.'
                    )

                # Case #2:
                # NVMe device is missing, add it (triggering a recovery)
                raid1_device = raid1_details['device']
                logging.info(f'Attaching {nvme} to md device {raid1_device}')
                run(['mdadm', '--manage', f'/dev/{raid1_device}', '--add', nvme])
                # wait for the RAID1 to start the rebuild to the RAID0 of NVMe
                while True:
                    raid1_details = md_device_details('raid1', mdev[0])
                    if raid1_details['state'] != 'idle':
                        break
                    raid1_device = raid1_details['device']
                    raid1_state = raid1_details['state']
                    logging.info(f'Waiting for array {raid1_device} to be start rebuild (currently "{raid1_state}")')
                    time.sleep(1)
            elif 'faulty' in mdraid1_device_nvme[0]['state']:
                raise CircuitBreakError(
                    'The RAID1 is active, but degraded with NVMe marked as faulty. Power-cycle the VM to get fresh NVMes.'
                )
            elif mdraid1_device_nvme[0]['device'] != device_nvme:
                # Some unexpected device, something is very wrong.
                mdraid1_device_unexpected = mdraid1_device_nvme[0]['device']
                raise CircuitBreakError(
                    f'The RAID1 is online with an unexpected device, has {mdraid1_device_unexpected} but should have {device_nvme}; circuit-breaking the scylla-server.service startup to prevent data loss!'
                )
    else:
        # RAID1 does not exist, create it with PD as first device
        logging.info(f'Creating md device mirroring {nvme} and {ssd}')
        command = [
            'mdadm',
            '--create',
            '/dev/md10',
            '--run',  # pd-ssd may be a different size than the NVMes
            '--level=1',
            '--bitmap=none',
            '--raid-devices=2',
            ssd,
            nvme,
        ]
        run(command)

        # Mark PD as write-mostly device
        ssd_component = ssd.split('/')[-1]
        logging.info(f'Marking md device /dev/md10 PD-SSD member device {ssd_component} as write-mostly...')
        with open(f'/sys/block/md10/md/dev-{ssd_component}/state', 'w') as pdssd_state_fp:
            pdssd_state_fp.write('writemostly')

    mdev = md_device('1')
    assert mdev is not None  # we just created a device (plz)

    # set sync_speed to be very high to ensure this completes quickly
    # as the service is not up at this point and we are not interfering
    with open(f'/sys/block/{mdev[0]}/md/sync_speed_max', 'w') as handle:
        # 3GBs is much higher than the current 24x NVMe will get, this was
        # chosen arbitrarily but intentionally much higher
        handle.write('3000000')

    # we want to make sure the array is stable before doing anything else
    # (we reaaally don't want to have to read from the pd-ssd device)
    while True:
        raid1_details = md_device_details('raid1', mdev[0])
        if raid1_details['state'] == 'idle':
            break
        if raid1_details['state'] == 'check':
            logging.warning(f'Array {mdev[0]} is currently checking; continuing with normal startup.')
            break

        # ensure the raid1 is NOT rebuilding from the RAID0 of NVME devices!
        mdraid1_device_pdssd = list(filter(lambda x: x['device'] == ssd.split('/')[-1], raid1_details['members']))
        if 'spare' in mdraid1_device_pdssd[0]['state']:
            # create a flag file to prevent further startup attempts until an operator can address this node
            raid1_device = raid1_details['device']
            device_pdssd = mdraid1_device_pdssd[0]['device']
            mdraid1_device_nvme = next(
                filter(lambda x: x['device'] != ssd.split('/')[-1], raid1_details['members']), None
            )
            device_nvme = mdraid1_device_nvme['device'] if mdraid1_device_nvme is not None else None
            write_circuit_break(f'mdraid1 /dev/{raid1_device} wrote from {device_nvme} to {device_pdssd}')
            raise CircuitBreakError(
                'The RAID1 is online but rebuilding from the PD-SSD; setting a persistent circuit-break flag file and aborting the scylla-server.service startup to prevent data loss!'
            )

        if raid1_details['state'] in ('resync', 'recovery'):
            global trigger_post_rebuild_reboot
            trigger_post_rebuild_reboot = True

        # update log and wait a bit
        raid1_state = raid1_details['state']
        logging.info(f'Waiting for array {mdev[0]} to be idle (currently "{raid1_state}")')
        logging.info(raid1_details)
        time.sleep(30)

    # set sync_speed roughly based on disk size to allow a check cron to run weekly
    # without severely interfering with the normal operation of the service
    # (we estimate the disk size based on the md size - it should be about the same)
    with open(f'/sys/block/{mdev[0]}/md/component_size', 'r') as handle:
        size = int(handle.read()) * 512
        bandwidth = int(_ioperf_gcp_ssd(size).write_bandwidth / 1024)
    with open(f'/sys/block/{mdev[0]}/md/sync_speed_max', 'w') as handle:
        handle.write(str(bandwidth))

    return f'/dev/{mdev[0]}'


def ensure_device_cached(nvmes: str, ssd: str):
    # some explanation:
    # - dm-cache needs a third device on which it stores metadata about cache state
    #   we use the nvmes for this because if the nvmes wipe, we want the metadata to also wipe
    #   this is achieved by creating two virtual devices using dm-linear on top of the nvmes
    # - we want to never have to read from the ssd during regular operations,
    #   so we dd the entire cache device to nowhere in order to force all data onto the nvmes
    #   and we mark that we've done this using the very first block of the nvme device
    #   (for the same reasons as why we use the nvmes for metadata)
    #   if the magic number is set in block 0, we know we don't have to dd again
    #   (since dd is expensive and delays scylla startup)
    MAGIC_NUMBER = b'SCYLLAMAKEHAPPY'  # :)
    METADATA_SIZE = 65536  # 32mb is big enough for, like, 16tb of data
    # metadata device
    if not os.path.exists('/dev/mapper/metadata'):
        logging.info('Creating cache metadata device on top of nvmes')
        run(['dmsetup', 'create', 'metadata', '--table', f'0 {METADATA_SIZE - 8} linear {nvmes} 8'])
    # data device
    if not os.path.exists('/dev/mapper/data'):
        logging.info('Creating cache data device on top of nvmes')
        size = int(run(['blockdev', '--getsz', nvmes]).strip()) - METADATA_SIZE
        run(['dmsetup', 'create', 'data', '--table', f'0 {size} linear {nvmes} {METADATA_SIZE}'])
    # cache device
    if not os.path.exists('/dev/mapper/cache'):
        logging.info('Creating cache device')
        size_data = int(run(['blockdev', '--getsz', '/dev/mapper/data']).strip())
        size_origin = int(run(['blockdev', '--getsz', ssd]).strip())
        size = min(size_data, size_origin)  # never provide more space than the nvmes have
        run(
            [
                'dmsetup',
                'create',
                'cache',
                '--table',
                f'0 {size} cache /dev/mapper/metadata /dev/mapper/data {ssd} 2097152 1 writethrough smq 0',
            ]
        )
    with open(nvmes, 'rb') as handle:
        # read may return fewer bytes, but it really, really shouldn't at this small size
        is_prepped = handle.read(len(MAGIC_NUMBER)) == MAGIC_NUMBER
    if is_prepped:
        logging.info('Cache device /dev/mapper/cache already prepped')
    else:
        # ensure the cache is prepped by dding the !@#$ out of it
        logging.info('Performing full read of cache device /dev/mapper/cache to prep nvmes')
        run(['dd', 'if=/dev/mapper/cache', 'of=/dev/null', 'bs=1048576'])
        # mark the cache as prepped
        with open(nvmes, 'wb') as handle:
            handle.write(MAGIC_NUMBER)
    return '/dev/mapper/cache'


def ensure_fs(device: str) -> str:
    """Ensure the device has xfs and return the device UUID"""
    try:
        output = run(['blkid', device])
    except subprocess.CalledProcessError:
        output = ''
    if 'TYPE="xfs"' not in output:
        logging.info(f'Creating xfs fs on {device}')
        run(['mkfs.xfs', device])
    output = run(['blkid', device])
    assert 'TYPE="xfs"' in output  # we just created this filesystem (plz)
    match = re.search(r'UUID="([^"]+)"', output)
    assert match  # xfs block devices have to have a UUID (plz)
    return match.group(1)


def _mount_info(mount_source: str, path_filter: str) -> Optional[dict]:
    """From a source like /proc/self/mounts or /etc/fstab, get mount details about a path"""
    with open(mount_source) as mount_fp:
        # get a list of arrays that's split up by whitespace #
        mount_arrays = map(lambda y: re.split(r'\s+', y.rstrip()), mount_fp.readlines())
        # filter out the list of arrays by any that don't start with a comment and contain more than 6 entries:
        # (comments will make this longer than 6 entries)
        # LABEL=cloudimg-rootfs   /        ext4   defaults        0 1 # root file system
        mount_candidates = list(filter(lambda x: len(x) >= 6 and '#' not in x[0] and x[1] == path_filter, mount_arrays))
        if len(mount_candidates) == 0:
            return None
        elif len(mount_candidates) > 1:
            raise Exception(f'unexpected count of {path_filter} mounts: {mount_candidates}')
    mount_candidates[0][3] = mount_candidates[0][3].split(',')
    return dict(zip(('device', 'mountpoint', 'type', 'options', 'dump', 'fsck_order'), mount_candidates[0]))


def ensure_mount(uuid: str) -> None:
    """Ensure the device is mounted (right now and in fstab)"""
    # update fstab
    scylla_fstab = _mount_info('/etc/fstab', '/var/lib/scylla')
    if (
            scylla_fstab is None
            or scylla_fstab['device'] != f'UUID={uuid}'
            or scylla_fstab['options'] != ['defaults', 'noatime', 'nofail']
    ):
        # unmount scylla mount points first
        unmount_scylla()

        # insert/update fstab with changes
        fstab = f'UUID={uuid} /var/lib/scylla xfs defaults,noatime,nofail 0 0'
        logging.info(f'Updating fstab: {fstab}')
        replace_line('/etc/fstab', r'\s\/var\/lib\/scylla\s', fstab)
    else:
        logging.info('fstab is up to date with Scylla mount device and options')

    # actually mount
    scylla_mount = _mount_info('/proc/self/mounts', '/var/lib/scylla')
    if scylla_mount is None:
        logging.info(f'Mounting UUID={uuid} to /var/lib/scylla')
        # holy **** systemd **** piece of ****
        # https://unix.stackexchange.com/questions/169909/
        sed_cmd = f'export MD_RAID1_UUID={uuid}; sed -i "s/What=\/dev\/disk\/by-uuid\/[^ ]*/What=\/dev\/disk\/by-uuid\/$MD_RAID1_UUID/" /etc/systemd/system/var-lib-scylla.mount'
        run_shell(sed_cmd)
        run(['systemctl', 'daemon-reload'])
        run(['systemctl', 'restart', 'var-lib-scylla.mount'])
        run(['chown', 'scylla:scylla', '-R', '/var/lib/scylla'])
    else:
        logging.info('Scylla mount point is already mounted')


def ensure_coredump() -> None:
    """Ensure the coredump subdirectory exists AND that it's bind mounted properly"""
    # only mkdir the coredump directory and mount it if the /var/lib/scylla directory is a mountpoint
    if _mount_info('/proc/self/mounts', '/var/lib/scylla') is None:
        logging.info('not setting up coredump bind mount due to no mountpoint found on: /var/lib/scylla')
        return

    # ensure the coredump sub-directory is created and properly owned
    if os.path.isdir('/var/lib/scylla/coredump'):
        logging.info('coredump sub-directory is already created: /var/lib/scylla/coredump')
    else:
        logging.info('creating coredump sub-directory: /var/lib/scylla/coredump')
        os.mkdir('/var/lib/scylla/coredump')
    run(['chown', 'scylla:scylla', '/var/lib/scylla/coredump'])

    # mount the coredump directory as well because dumps won't fit onto the root fs
    if _mount_info('/proc/self/mounts', '/var/lib/systemd/coredump') is None:
        logging.info('bind mounting coredump directory from /var/lib/scylla/coredump to: /var/lib/systemd/coredump')
        run(['mount', '--bind', '/var/lib/scylla/coredump', '/var/lib/systemd/coredump'])
    else:
        logging.info('bind mount, for coredump, is already mounted')


def ensure_not_running_with_circuit_break_flagfile():
    """A flag file will be created if the RAID1 has ever attempted a rebuild from the RAID0 NVMe"""
    # this flag file must be manually removed if an operator determines this is/was OK
    global CIRCUIT_BREAK_FILE
    if os.path.exists(CIRCUIT_BREAK_FILE):
        with open(CIRCUIT_BREAK_FILE) as breaker_fp:
            reason = breaker_fp.readline().rstrip()
        raise CircuitBreakError(
            f'A persistent circuit-break flag file, {CIRCUIT_BREAK_FILE}, exists -- stopping the scylla-server.service startup to prevent data loss: {reason}'
        )


def write_circuit_break(information: str):
    """Writes a circuit-break flag file"""
    if os.path.exists(CIRCUIT_BREAK_FILE):
        logging.warning(f'Circuit break file already exists -- not overwritting with new information: {information}')
        return
    with open(CIRCUIT_BREAK_FILE, 'w') as breaker_fp:
        breaker_fp.write(f'{information} on: {datetime.datetime.utcnow()}UTC')


# Identify the UUID of the SSD device /dev/sdb
def get_ssd_uuid(device_path):
    try:
        command = ["blkid", "-o", "value", "-s", "UUID", device_path]
        uuid = run(command).strip()
        return uuid
    except subprocess.CalledProcessError:
        return None


# Create a udev rule file with the identified UUID
def create_udev_rule(uuid, device_name):
    udev_rule = f'SUBSYSTEM=="block", ENV{{ID_SERIAL_SHORT}}=="{uuid}", SYMLINK+="{device_name}"'
    with open(f'/etc/udev/rules.d/99-{device_name}.rules', 'w') as rule_file:
        rule_file.write(udev_rule)


# Reload udev rules
def reload_udev_rules():
    run(["udevadm", "control", "--reload-rules"])
    run(["udevadm", "trigger"])


def make_ssd_raid_device_persistent():
    device_path = '/dev/sdb'  # Change this to the actual device path
    device_name = 'ssd_device'  # Change this to the desired device name
    uuid = get_ssd_uuid(device_path)

    if uuid:
        create_udev_rule(uuid, device_name)
        reload_udev_rules()
        logging.info(f"UUID: {uuid} - Created udev rule for {device_path} as {device_name}")
    else:
        logging.error(f"Failed to retrieve UUID for {device_path}")


def disable_nic_and_disks_configuration():
    # Disable running perftune.py since it doesn't recognize mdadm device.
    config_file = "/etc/default/scylla-server"
    config_values = {
        "SET_NIC_AND_DISKS": "no",
        "SET_CLOCKSOURCE": "no",
        "DISABLE_WRITEBACK_CACHE": "no"
    }

    try:
        for key, new_value in config_values.items():
            # Use your custom "run" function here
            run(['sudo', 'sed', '-i', f's/{key}=.*/{key}={new_value}/', config_file])
            logging.info(f"Configuration in {config_file} updated to: {key}={new_value}")
    except Exception as e:
        logging.error(f"Error updating configuration: {e}")


def unmount_scylla():
    scylla_mount = _mount_info('/proc/self/mounts', '/var/lib/scylla')
    if scylla_mount is not None:
        run(['systemctl', 'stop', 'var-lib-scylla.mount'])
        coredump_mount = _mount_info('/proc/self/mounts', '/var/lib/systemd/coredump')
        if coredump_mount is not None and coredump_mount['device'] == scylla_mount['device']:
            run(['umount', '/var/lib/systemd/coredump'])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--nvme-raid-level', default='0', choices=['0', '5'], help='NVMe raid level')
    # how (if) to handle the PD SSD #
    group_pd_type = parser.add_mutually_exclusive_group()
    group_pd_type.add_argument('--write-mostly-device', help='the (optional) device mirroring the nvmes')
    group_pd_type.add_argument('--write-through-device', help='the (optional) device for the nvmes to cache')
    # excludes the PD SSD when doing operations like computing IO perf #
    group_ignore_from_perf = parser.add_mutually_exclusive_group()
    group_ignore_from_perf.add_argument('--raid-exclude-pd-from-perf', action='store_true')
    group_ignore_from_perf.add_argument('--no-raid-exclude-pd-from-perf', action='store_false')
    group_ignore_from_perf.set_defaults(raid_exclude_pd_from_perf=False)
    # ignores the state of the RAID1 (but it HAS to be up) #
    group_ignore_raid1_state = parser.add_mutually_exclusive_group()
    group_ignore_raid1_state.add_argument('--ignore-raid1-state', action='store_true')
    group_ignore_raid1_state.add_argument('--no-ignore-raid1-state', action='store_false')
    group_ignore_raid1_state.set_defaults(ignore_raid1_state=False)
    # io duplexing #
    group_duplex = parser.add_mutually_exclusive_group()
    group_duplex.add_argument('--duplex', action='store_true')
    group_duplex.add_argument('--no-duplex', action='store_false')
    group_duplex.set_defaults(duplex=False)
    args = parser.parse_args()

    ensure_not_running_with_circuit_break_flagfile()

    pdssd_device = os.getenv('PREFLIGHT_DEVICE_PDSSD', '/dev/sdb')
    nvmes = get_nvmes()
    if nvmes:
        # get the max speed as currently set
        # it's going to be artifically raised so it'll need to be restored
        with open('/proc/sys/dev/raid/speed_limit_max', 'r', encoding="utf-8") as handle:
            speed_limit_max = handle.readline().rstrip()

        device_striped = ensure_device_striped(nvmes, args.nvme_raid_level)
        ioperf = get_ioperf(device_striped, None)

        unmount_scylla()
        device_filesystem = None
        if args.write_mostly_device is not None:
            # if specified on the command line options, this will override the environment variable: PREFLIGHT_DEVICE_PDSSD
            pdssd_device = args.write_mostly_device
            pdssd_device = pdssd_device if '/' in pdssd_device else f'/dev/{pdssd_device}'
            ioperf = get_ioperf(device_striped, pdssd_device if not args.raid_exclude_pd_from_perf else None)
            device_filesystem = ensure_device_mirrored(device_striped, pdssd_device, args.ignore_raid1_state)
        if args.write_through_device is not None:
            wtd = args.write_through_device
            wtd = wtd if '/' in wtd else f'/dev/{wtd}'
            ioperf = get_ioperf(device_striped, wtd)
            device_filesystem = ensure_device_cached(device_striped, wtd)

        # if no write-mostly and no write-through, then we'll need to set the device_filesystem
        # to the striped device directly
        if device_filesystem is None:
            device_filesystem = device_striped

        apply_striped_speed_limit(device_filesystem, args.nvme_raid_level)

        # restore the max speed #
        with open('/proc/sys/dev/raid/speed_limit_max', 'w', encoding="utf-8") as handle:
            handle.write(speed_limit_max)
    else:
        device_filesystem = pdssd_device
        ioperf = get_ioperf(None, device_filesystem)
    update_io_properties(ioperf, args.duplex)
    uuid = ensure_fs(device_filesystem)
    logging.info(f'RAID-1 UUID is: ({uuid})')
    ensure_mount(uuid)
    ensure_coredump()
    make_ssd_raid_device_persistent()
    disable_nic_and_disks_configuration()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
