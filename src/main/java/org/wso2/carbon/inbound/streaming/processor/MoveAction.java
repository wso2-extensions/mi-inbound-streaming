/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.streaming.processor;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.streaming.Utils;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.wso2.carbon.inbound.streaming.VFSConstants;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static org.wso2.carbon.inbound.streaming.Utils.extractPath;
import static org.wso2.carbon.inbound.streaming.Utils.sanitizeFileUriWithSub;

/**
 * Move action to move the file to a different location.
 */
public class MoveAction implements Action {

    private final Log log = LogFactory.getLog(MoveAction.class);
    String targetLocation;
    VFSConfig vfsConfig;
    FileSystemManager fsManager;

    public MoveAction(String targetLocation, VFSConfig vfsConfig, FileSystemManager fsManager) {
        this.targetLocation = targetLocation;
        this.vfsConfig = vfsConfig;
        this.fsManager = fsManager;
    }

    @Override
    public void execute(FileObject fileObject) throws FileSystemException {
        // after success
        FileSystemOptions destinationFSO = null;
        Map<String, String> query = Utils.parseSchemeFileOptions(targetLocation, new Properties());
        if (query != null) {
            query.putAll(vfsConfig.getVfsSchemeProperties());
        }
        targetLocation = Utils.stripVfsSchemeIfPresent(targetLocation);
        targetLocation = extractPath(targetLocation);

        try {
            destinationFSO = Utils.attachFileSystemOptions(query, fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the options for processed file location ", e);
        }

        if (targetLocation != null && targetLocation.trim().length() > 0) {
            if (Utils.supportsSubDirectoryToken(targetLocation)) {
                targetLocation = sanitizeFileUriWithSub(targetLocation);
            }

            log.info("Moving file to " + targetLocation);

            if (vfsConfig.getSubfolderTimestamp() != null) {
                targetLocation = Utils.optionallyAppendDateToUri(targetLocation, vfsConfig);
            }

            FileObject moveToDirectory = fsManager.resolveFile(targetLocation, destinationFSO);
            // Settle the final name before anything touches the destination, so the folder
            // creation and the last-modified flag below apply to the file actually written.
            FileObject dest = moveToDirectory.resolveFile(destinationFileName(fileObject));
            // transport.vfs.CreateFolder, as an inbound parameter or a query parameter on the URI.
            boolean forceCreate = vfsConfig.isForceCreateFolder();
            if (query != null && !StringUtils.isEmpty(query.get(VFSConstants.FORCE_CREATE_FOLDER))) {
                forceCreate = Boolean.parseBoolean(query.get(VFSConstants.FORCE_CREATE_FOLDER));
            }
            if (forceCreate) {
                dest.createFile();
            }

            dest.setUpdateLastModified(vfsConfig.isUpdateLastModified());

            fileObject.moveTo(dest);
            log.info("File moved");

        } else {
            log.info("Target location not provided. Hence not moving the file");
        }
    }

    /**
     * The name the file is moved under: its own name, prefixed with the current time when
     * {@code transport.vfs.MoveTimestampFormat} is set (e.g. {@code 20261027-1015_orders.csv}), so
     * repeated runs do not overwrite each other in the move folder.
     */
    String destinationFileName(FileObject fileObject) {
        String baseName = fileObject.getName().getBaseName();
        DateFormat moveTimestampFormat = vfsConfig.getMoveTimestampFormat();
        if (moveTimestampFormat == null) {
            return baseName;
        }
        // The format instance is shared through VFSConfig, and DateFormat is not thread-safe.
        synchronized (moveTimestampFormat) {
            return moveTimestampFormat.format(new Date()) + "_" + baseName;
        }
    }
}
