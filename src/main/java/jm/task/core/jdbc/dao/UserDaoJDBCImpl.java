package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {}

    public void createUsersTable() {
        Connection connection = Util.getConnect();
        Statement statement = null;

        try {
            statement = connection.createStatement();

            statement.addBatch("CREATE TABLE IF NOT EXISTS User (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(99) NOT NULL, lastName VARCHAR(99) NOT NULL, age TINYINT NOT NULL, PRIMARY KEY (id))");
            statement.executeBatch();

            try {
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void dropUsersTable() {
        Connection connection = Util.getConnect();
        Statement statement = null;

        try {
            statement = connection.createStatement();

            statement.addBatch("DROP TABLE IF EXISTS User");
            statement.executeBatch();

            try {
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = Util.getConnect();
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement("INSERT INTO User(name, lastName, age) VALUES(?, ?, ?)");

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            preparedStatement.addBatch();
            preparedStatement.executeBatch();

            System.out.printf("User с именем \"%s\" добавлен в базу данных!", name);
            System.out.println();

            try {
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = Util.getConnect();
        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement("DELETE FROM User WHERE id = ?");

            preparedStatement.setLong(1, id);

            preparedStatement.addBatch();
            preparedStatement.executeBatch();

            try {
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        Connection connection = Util.getConnect();
        Statement statement = null;
        ResultSet res = null;

        try {
            statement = connection.createStatement();

            res = statement.executeQuery("SELECT * FROM User");

            while (res.next()) {
                User user = new User(res.getString("name"),
                                    res.getString("lastName"),
                                    res.getByte("age"));
                user.setId(res.getLong("id"));
                users.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (res != null) {
                    res.close();
                }

                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return users;
    }

    public void cleanUsersTable() {
        Connection connection = Util.getConnect();
        Statement statement = null;

        try {
            statement = connection.createStatement();

            statement.addBatch("TRUNCATE TABLE User");
            statement.executeBatch();

            try {
                connection.commit();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }

                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
