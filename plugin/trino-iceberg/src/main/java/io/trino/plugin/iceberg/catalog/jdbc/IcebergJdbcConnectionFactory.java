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
package io.trino.plugin.iceberg.catalog.jdbc;

import org.jdbi.v3.core.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IcebergJdbcConnectionFactory
        implements ConnectionFactory
{
    private final String connectionUrl;
    private final Optional<String> user;
    private final Optional<String> password;

    public IcebergJdbcConnectionFactory(String connectionUrl, Optional<String> user, Optional<String> password)
    {
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(connectionUrl, user.orElse(null), password.orElse(null));
        checkState(connection != null, "Driver returned null connection, make sure the connection URL is valid");
        return connection;
    }
}
