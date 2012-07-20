# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 Nicolas de Bonfils openstack@nicolas2bonfils.com
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

#TODO(nicolas) : i18n the log message

from cinder.volume.driver import *

class ZVolDriver(ISCSIDriver):
    """
    Drivers for volumes using Zvol instead of LVM.
    Tested with ZFSOnLinux on a Debian sid system

    """

    def __init__(self, *args, **kwargs):
        #TODO(nicolas) : use configuration flag
        self.zfs_bin = "/sbin/zfs"
        self.zpool_bin = "/sbin/zpool"
        super(ZVolDriver, self).__init__(*args, **kwargs)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        out, err = self._execute('%s' % self.zfs_bin, 'list', run_as_root=True)
        volume_groups = out.split()
        if not FLAGS.volume_group in volume_groups:
            raise exception.Error(_("zfs base volume group '%s' doesn't exist")
                                  % FLAGS.volume_group)

    def _create_volume(self, volume_name, sizestr):
        LOG.debug('Create volume with command "%s create -V %s %s/%s"' % (self.zfs_bin, sizestr, FLAGS.volume_group, volume_name))
        self._try_execute('%s' % self.zfs_bin, 'create', 
						  '-V', sizestr,
                          '%s/%s' % (FLAGS.volume_group, volume_name), 
                          run_as_root=True)

    def _copy_volume(self, srcstr, deststr, size_in_g):
        LOG.debug('copy volume with method "dd if=%s of=%s count=%d bs=1M"' % (srcstr, deststr, (size_in_g * 1024)))
        self._execute('dd', 'if=%s' % srcstr, 'of=%s' % deststr,
                      'count=%d' % (size_in_g * 1024), 'bs=1M',
                      run_as_root=True)

    def _volume_not_present(self, volume_name):
        path_name = '%s/%s' % (FLAGS.volume_group, volume_name)
        out, err = self._execute('%s' % self.zfs_bin, 'list', run_as_root=True)
        volume_search = out.split()
        LOG.debug('List volume with command "%s list"' % self.zfs_bin)
        LOG.debug(volume_search)
        return not path_name in volume_search

    def _delete_volume(self, volume, size_in_g):
        """Deletes a zvol."""
        # zero out old volumes to prevent data leaking between users
        # TODO(ja): reclaiming space should be done lazy and low priority
        self._copy_volume('/dev/zero', self.local_path(volume), size_in_g)
        LOG.debug('Destroy volume with command "%s destroy %s/%s"' % (self.zfs_bin, FLAGS.volume_group, volume['name']))
        self._try_execute('%s' % self.zfs_bin, 'destroy', 
                          '%s/%s' % (FLAGS.volume_group, volume['name']),
                          run_as_root=True)

    def delete_volume(self, volume):
        """Deletes a zvol."""
        if self._volume_not_present(volume['name']):
            # If the volume isn't present, then don't attempt to delete
            return True

		# When snapshots exist, what to do ?
		# remove them or export them as new independent volume
         
        self._delete_volume(volume, volume['size'])

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""
        LOG.debug('Snapshot volume with command "%s snapshot %s/%s"' % (self.zfs_bin, 
                                                                        FLAGS.volume_group, 
                                                                        self._snapshot_full_name(snapshot)))
        self._try_execute('%s' % self.zfs_bin, 'snapshot',
                          '%s/%s' % (FLAGS.volume_group, self._snapshot_full_name(snapshot)), 
                          run_as_root=True)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""
        if self._volume_not_present(self._snapshot_full_name(snapshot)):
            # If the snapshot isn't present, then don't attempt to delete
            return True

        self._delete_volume(snapshot, snapshot['volume_size'])

    def local_path(self, volume):
        """Give full path to the system /dev object"""
        full_name = ""
        # if we got a volume name, then it's a snapshot so create the full path
        # otherwise it a "regular" volume
        
        # it's a hack to know if volume "respond to" volume_name key.
        #FIXME(nicolas) : volume is sqlalchemy object, how to query existence for a property/method ?
        try:
            volume_name = volume['volume_name']
            full_name = self._snapshot_full_name(volume)
        except Exception as e:
            full_name = volume['name']
        return "/dev/zvol/%s/%s" % (FLAGS.volume_group, full_name)
        
    def _snapshot_full_name(self, snapshot):
        """Give the snapshot full name : volume name + snapshot name with '@' in the middle """
        return '%s@%s' % (snapshot['volume_name'], snapshot['name'])