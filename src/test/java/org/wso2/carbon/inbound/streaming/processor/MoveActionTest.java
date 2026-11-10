/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.wso2.carbon.inbound.streaming.VFSConfig;
import org.wso2.carbon.inbound.streaming.VFSConstants;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MoveActionTest {

    private static FileSystemManager fsManager;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void setUp() throws Exception {
        fsManager = VFS.getManager();
    }

    private static VFSConfig config(String... keyValues) {
        Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return new VFSConfig(p);
    }

    private File source(String name) throws Exception {
        File in = tmp.newFolder("in");
        File f = new File(in, name);
        Files.write(f.toPath(), "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
        return f;
    }

    private void move(File file, File targetDir, VFSConfig cfg, String query) throws Exception {
        FileObject fo = fsManager.resolveFile(file.toURI().toString());
        String target = targetDir.toURI().toString() + (query == null ? "" : "?" + query);
        new MoveAction(target, cfg, fsManager).execute(fo);
    }

    private static String[] names(File dir) {
        String[] names = dir.list();
        Arrays.sort(names);
        return names;
    }

    @Test
    public void movesTheFileIntoTheTargetFolder() throws Exception {
        File src = source("orders.csv");
        File out = tmp.newFolder("out");
        move(src, out, config(), null);
        assertFalse(src.exists());
        assertArrayEquals(new String[]{"orders.csv"}, names(out));
    }

    @Test
    public void timestampFormatPrefixesAFormattedDateInTheTargetFolder() throws Exception {
        File src = source("orders.csv");
        File out = tmp.newFolder("out");
        String pattern = "yyyyMMdd";
        String today = new SimpleDateFormat(pattern).format(new Date());

        move(src, out, config(VFSConstants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT, pattern), null);

        // Previously: named "java.text.SimpleDateFormat@..._orders.csv" and left next to the source.
        assertFalse(src.exists());
        assertArrayEquals(new String[0], names(src.getParentFile()));
        String[] moved = names(out);
        assertEquals(1, moved.length);
        assertTrue(moved[0], moved[0].equals(today + "_orders.csv")
            // the test straddled midnight
            || moved[0].endsWith("_orders.csv") && moved[0].length() == today.length() + 11);
    }

    @Test
    public void createFolderLeavesNoPlaceholderBehindWhenRenaming() throws Exception {
        File src = source("orders.csv");
        File out = new File(tmp.getRoot(), "out/nested");
        String pattern = "yyyyMMdd";

        move(src, out, config(VFSConstants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT, pattern),
            VFSConstants.FORCE_CREATE_FOLDER + "=true");

        // Only the renamed file: no empty "orders.csv" placeholder created under the old name.
        String[] moved = names(out);
        assertEquals(Arrays.toString(moved), 1, moved.length);
        assertTrue(moved[0], moved[0].endsWith("_orders.csv"));
        assertEquals("a,b\n1,2\n",
            new String(Files.readAllBytes(new File(out, moved[0]).toPath()), StandardCharsets.UTF_8));
    }
}
