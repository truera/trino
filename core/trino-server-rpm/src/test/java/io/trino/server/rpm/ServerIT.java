/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.rpm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.sql.DriverManager.getConnection;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

@Execution(SAME_THREAD)
public class ServerIT
{
    private static final String BASE_IMAGE_PREFIX = "eclipse-temurin:";
    private static final String BASE_IMAGE_SUFFIX = "-jre-ubi9-minimal";

    private final String rpmHostPath;

    public ServerIT()
    {
        rpmHostPath = requireNonNull(System.getProperty("rpm"), "rpm is null");
    }

    @Test
    public void testInstall()
    {
        testInstall("17");
        testInstall("21");
    }

    private void testInstall(String javaVersion)
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        String command = "" +
                // install required dependencies that are missing in UBI9-minimal
                "microdnf install -y python sudo\n" +
                // install RPM
                "rpm -i " + rpm + "\n" +
                // create Hive catalog file
                "mkdir /etc/trino/catalog\n" +
                "echo CONFIG_ENV[HMS_PORT]=9083 >> /etc/trino/env.sh\n" +
                "echo CONFIG_ENV[NODE_ID]=test-node-id-injected-via-env >> /etc/trino/env.sh\n" +
                "sed -i \"s/^node.id=.*/node.id=\\${ENV:NODE_ID}/g\" /etc/trino/node.properties\n" +
                "cat > /etc/trino/catalog/hive.properties <<\"EOT\"\n" +
                "connector.name=hive\n" +
                "hive.metastore.uri=thrift://localhost:${ENV:HMS_PORT}\n" +
                "EOT\n" +
                // create JMX catalog file
                "cat > /etc/trino/catalog/jmx.properties <<\"EOT\"\n" +
                "connector.name=jmx\n" +
                "EOT\n" +
                // start server
                "/etc/init.d/trino start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/trino/server.log\n";

        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE_PREFIX + javaVersion + BASE_IMAGE_SUFFIX)) {
            container.withExposedPorts(8080)
                    // the RPM is hundreds MB and file system bind is much more efficient
                    .withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", command)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            QueryRunner queryRunner = new QueryRunner(container.getHost(), container.getMappedPort(8080));
            assertThat(queryRunner.execute("SHOW CATALOGS")).isEqualTo(ImmutableSet.of(asList("system"), asList("hive"), asList("jmx")));
            assertThat(queryRunner.execute("SELECT node_id FROM system.runtime.nodes")).isEqualTo(ImmutableSet.of(asList("test-node-id-injected-via-env")));
            // TODO remove usage of assertEventually once https://github.com/trinodb/trino/issues/2214 is fixed
            assertEventually(
                    new io.airlift.units.Duration(1, MINUTES),
                    () -> assertThat(queryRunner.execute("SELECT specversion FROM jmx.current.\"java.lang:type=runtime\"")).isEqualTo(ImmutableSet.of(asList(javaVersion))));
        }
    }

    @Test
    public void testUninstall()
            throws Exception
    {
        testUninstall("17");
        testUninstall("21");
    }

    private void testUninstall(String javaVersion)
            throws Exception
    {
        String rpm = "/" + new File(rpmHostPath).getName();
        String installAndStartTrino = "" +
                // install required dependencies that are missing in UBI9-minimal
                "microdnf install -y python sudo\n" +
                // install RPM
                "rpm -i " + rpm + "\n" +
                "/etc/init.d/trino start\n" +
                // allow tail to work with Docker's non-local file system
                "tail ---disable-inotify -F /var/log/trino/server.log\n";
        try (GenericContainer<?> container = new GenericContainer<>(BASE_IMAGE_PREFIX + javaVersion + BASE_IMAGE_SUFFIX)) {
            container.withFileSystemBind(rpmHostPath, rpm, BindMode.READ_ONLY)
                    .withCommand("sh", "-xeuc", installAndStartTrino)
                    .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)))
                    .start();
            String uninstallTrino = "" +
                    "/etc/init.d/trino stop\n" +
                    "rpm -e trino-server-rpm\n";
            container.execInContainer("sh", "-xeuc", uninstallTrino);

            ExecResult actual = container.execInContainer("rpm", "-q", "trino-server-rpm");
            assertThat(actual.getStdout()).isEqualTo("package trino-server-rpm is not installed\n");

            assertPathDeleted(container, "/var/lib/trino");
            assertPathDeleted(container, "/usr/lib/trino");
            assertPathDeleted(container, "/etc/init.d/trino");
            assertPathDeleted(container, "/usr/shared/doc/trino");
        }
    }

    private static void assertPathDeleted(GenericContainer<?> container, String path)
            throws Exception
    {
        ExecResult actualResult = container.execInContainer(
                "sh",
                "-xeuc",
                format("test -d %s && echo -n 'path exists' || echo -n 'path deleted'", path));
        assertThat(actualResult.getStdout()).isEqualTo("path deleted");
        assertThat(actualResult.getExitCode()).isEqualTo(0);
    }

    private static class QueryRunner
    {
        private final String host;
        private final int port;

        private QueryRunner(String host, int port)
        {
            this.host = requireNonNull(host, "host is null");
            this.port = port;
        }

        public Set<List<String>> execute(String sql)
        {
            try (Connection connection = getConnection(format("jdbc:trino://%s:%s", host, port), "test", null);
                    Statement statement = connection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(sql)) {
                    ImmutableSet.Builder<List<String>> rows = ImmutableSet.builder();
                    int columnCount = resultSet.getMetaData().getColumnCount();
                    while (resultSet.next()) {
                        ImmutableList.Builder<String> row = ImmutableList.builder();
                        for (int column = 1; column <= columnCount; column++) {
                            row.add(resultSet.getString(column));
                        }
                        rows.add(row.build());
                    }
                    return rows.build();
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
