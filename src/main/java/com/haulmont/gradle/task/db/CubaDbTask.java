/*
 * Copyright (c) 2008-2017 Haulmont.
 *
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

package com.haulmont.gradle.task.db;

import groovy.lang.GroovyObject;
import groovy.sql.Sql;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import org.apache.commons.text.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class CubaDbTask extends DefaultTask {
    public static final String POSTGRES_DBMS = "postgres";
    public static final String MSSQL_DBMS = "mssql";
    public static final String ORACLE_DBMS = "oracle";
    public static final String MYSQL_DBMS = "mysql";
    public static final String HSQL_DBMS = "hsql";

    protected static final String SQL_COMMENT_PREFIX = "--";
    protected static final String CURRENT_SCHEMA_PARAM = "currentSchema";
    protected static final String MS_SQL_2005 = "2005";

    protected String dbms;
    protected String dbmsVersion;
    protected String delimiter = "^";
    protected String host = "localhost";
    protected String dbFolder = "db";
    protected String connectionParams = "";
    protected String dbName;
    protected String dbUser;
    protected String dbPassword;
    protected String driverClasspath;
    protected String dbUrl;
    protected String driver;
    protected String timeStampType;
    protected File dbDir;
    protected Sql sqlInstance;

    private final Logger log = LoggerFactory.getLogger(CubaDbTask.class);

    public String getDbms() {
        return dbms;
    }

    public void setDbms(String dbms) {
        this.dbms = dbms;
    }

    public String getDbmsVersion() {
        return dbmsVersion;
    }

    public void setDbmsVersion(String dbmsVersion) {
        this.dbmsVersion = dbmsVersion;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDbFolder() {
        return dbFolder;
    }

    public void setDbFolder(String dbFolder) {
        this.dbFolder = dbFolder;
    }

    public String getConnectionParams() {
        return connectionParams;
    }

    public void setConnectionParams(String connectionParams) {
        if (connectionParams == null) {
            connectionParams = "";
        }
        this.connectionParams = connectionParams;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDriverClasspath() {
        return driverClasspath;
    }

    public void setDriverClasspath(String driverClasspath) {
        this.driverClasspath = driverClasspath;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getTimeStampType() {
        return timeStampType;
    }

    public void setTimeStampType(String timeStampType) {
        this.timeStampType = timeStampType;
    }

    public File getDbDir() {
        return dbDir;
    }

    public void setDbDir(File dbDir) {
        this.dbDir = dbDir;
    }

    protected void init() {
        Properties properties = initProperties();
        String dataSourceProvider = properties.getProperty("cuba.dataSourceProvider");
        if ("application".equals(dataSourceProvider)) {
            initApplicationDataSource(properties);
        } else {
            initDefaultDataSource();
        }

        Project project = getProject();
        dbDir = new File(project.getBuildDir(), dbFolder);

        initDriverClasspath(project);
    }

    protected void initDriverClasspath(Project project) {
        ClassLoader classLoader = GroovyObject.class.getClassLoader();
        if (StringUtils.isBlank(driverClasspath)) {
            driverClasspath = project.getConfigurations().getByName("jdbc").fileCollection(dependency -> true).getAsPath();
            project.getConfigurations().getByName("jdbc").fileCollection(dependency -> true).getFiles()
                    .forEach(file -> {
                        try {
                            Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                            addURLMethod.setAccessible(true);
                            addURLMethod.invoke(classLoader, file.toURI().toURL());
                        } catch (NoSuchMethodException | IllegalAccessException
                                | InvocationTargetException | MalformedURLException e) {
                            throw new GradleException("Exception when invoke 'java.net.URLClassLoader.addURL' method", e);
                        }

                    });
        } else {
            java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(driverClasspath, File.pathSeparator);
            while (tokenizer.hasMoreTokens()) {
                try {
                    URL url = new File(tokenizer.nextToken()).toURI().toURL();
                    Method addURLMethod = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    addURLMethod.setAccessible(true);
                    addURLMethod.invoke(classLoader, url);
                } catch (NoSuchMethodException | IllegalAccessException
                        | InvocationTargetException | MalformedURLException e) {
                    throw new GradleException("Exception when invoke 'java.net.URLClassLoader.addURL' method", e);
                }
            }
        }
        project.getLogger().info("[CubaDbTask] driverClasspath: " + driverClasspath);
    }

    protected void initDefaultDataSource() {
        if (StringUtils.isBlank(driver) || StringUtils.isBlank(dbUrl)) {
            initDataSource();
        }
    }

    protected void initApplicationDataSource(Properties properties) {
        dbUrl = properties.getProperty("cuba.dataSource.jdbcUrl");
        dbUser = properties.getProperty("cuba.dataSource.username");
        dbPassword = properties.getProperty("cuba.dataSource.password");
        dbName = properties.getProperty("cuba.dataSource.dbName");
        host = properties.getProperty("cuba.dataSource.host") + ":" + properties.getProperty("cuba.dataSource.port");
        connectionParams = properties.getProperty("cuba.dataSource.connectionParams") == null ?
        "" : properties.getProperty("cuba.dataSource.connectionParams");

        if (dbUrl != null) {
            initDataSourceByUrl();
        } else {
            initDataSource();
        }
    }

    protected void initDataSource() {
        if (POSTGRES_DBMS.equals(dbms)) {
            driver = "org.postgresql.Driver";
            dbUrl = "jdbc:postgresql://" + host + "/" + dbName + connectionParams;
            if (StringUtils.isBlank(timeStampType)) {
                timeStampType = "timestamp";
            }
        } else if (MSSQL_DBMS.equals(dbms)) {
            if (MS_SQL_2005.equals(dbmsVersion)) {
                driver = "net.sourceforge.jtds.jdbc.Driver";
                dbUrl = "jdbc:jtds:sqlserver://" + host + "/" + dbName + connectionParams;
            } else {
                driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                dbUrl = "jdbc:sqlserver://" + host + ";databaseName=" + dbName + connectionParams;
            }
            if (StringUtils.isBlank(timeStampType)) {
                timeStampType = "datetime";
            }
        } else if (ORACLE_DBMS.equals(dbms)) {
            driver = "oracle.jdbc.OracleDriver";
            dbUrl = "jdbc:oracle:thin:@//" + host + "/" + dbName + connectionParams;
            if (StringUtils.isBlank(timeStampType)) {
                timeStampType = "timestamp";
            }
        } else if (HSQL_DBMS.equals(dbms)) {
            driver = "org.hsqldb.jdbc.JDBCDriver";
            dbUrl = "jdbc:hsqldb:hsql://" + host + "/" + dbName + connectionParams;
            if (StringUtils.isBlank(timeStampType)) {
                timeStampType = "timestamp";
            }
        } else if (MYSQL_DBMS.equals(dbms)) {
            driver = "com.mysql.jdbc.Driver";
            if (StringUtils.isBlank(connectionParams)) {
                connectionParams = "?useSSL=false&allowMultiQueries=true&serverTimezone=UTC";
            }
            dbUrl = "jdbc:mysql://" + host + "/" + dbName + connectionParams;
            if (StringUtils.isBlank(timeStampType)) {
                timeStampType = "datetime";
            }
        } else
            throw new UnsupportedOperationException("DBMS " + dbms + " is not supported. " +
                    "You should either provide 'driver' and 'dbUrl' properties, or specify one of supported DBMS in 'dbms' property");
    }

    protected void initDataSourceByUrl() {
        if (dbUrl.contains("jdbc:postgresql://")) {
            driver = "org.postgresql.Driver";
            dbms = POSTGRES_DBMS;
            timeStampType = "timestamp";
        } else if (dbUrl.contains("jdbc:jtds:sqlserver://")) {
            driver = "net.sourceforge.jtds.jdbc.Driver";
            dbms = MSSQL_DBMS;
            dbmsVersion = MS_SQL_2005;
            timeStampType = "datetime";
        } else if (dbUrl.contains("jdbc:sqlserver://")) {
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            dbms = MSSQL_DBMS;
            timeStampType = "datetime";
        } else if (dbUrl.contains("jdbc:oracle:thin:@//")) {
            driver = "oracle.jdbc.OracleDriver";
            dbms = ORACLE_DBMS;
            timeStampType = "timestamp";
        } else if (dbUrl.contains("jdbc:hsqldb:hsql://")) {
            driver = "org.hsqldb.jdbc.JDBCDriver";
            dbms = HSQL_DBMS;
            timeStampType = "timestamp";
        } else if (dbUrl.contains("jdbc:mysql://")) {
            driver = "com.mysql.jdbc.Driver";
            dbms = MYSQL_DBMS;
            dbUrl = dbUrl + "?useSSL=false&allowMultiQueries=true&serverTimezone=UTC";
            timeStampType = "datetime";
        } else {
            throw new UnsupportedOperationException("DBMS " + dbms + " is not supported. " +
                    "You should either provide 'driver' and 'dbUrl' properties, or specify one of supported DBMS in 'dbms' property");
        }
    }

    protected void initDatabase(String oneModuleDir) {
        initDatabase(oneModuleDir, any -> true);
    }

    protected void initDatabase(String oneModuleDir, Function<File, Boolean> scriptFilter) {
        Project project = getProject();
        try {
            ScriptFinder scriptFinder = new ScriptFinder(dbms, dbmsVersion, dbDir, Collections.singletonList("sql"), project);

            List<File> initScripts = scriptFinder.getInitScripts(oneModuleDir)
                    .stream()
                    .filter(scriptFilter::apply)
                    .collect(Collectors.toList());

            initScripts.forEach(file -> {
                project.getLogger().warn("Executing SQL script: " + file.getAbsolutePath());
                executeSqlScript(file);
                String name = getScriptName(file);
                markScript(name, true);
            });
        } finally {
            // mark all update scripts as executed even in case of createDb failure
            ScriptFinder scriptFinder = new ScriptFinder(dbms, dbmsVersion, dbDir, Arrays.asList("sql", "groovy"), project);
            List<File> updateScripts = scriptFinder.getUpdateScripts(oneModuleDir);
            updateScripts.forEach(file -> {
                String name = getScriptName(file);
                markScript(name, true);
            });
        }
    }

    protected Properties initProperties() {
        String propsConfigName = getPropsConfigName();
        Properties properties = new Properties();

        StringTokenizer tokenizer = new StringTokenizer(propsConfigName);
        tokenizer.setQuoteChar('"');
        for (String str : tokenizer.getTokenArray()) {
            if (str.startsWith("classpath:")) {
                str = str.replace("classpath:", "");
                String[] paths = str.split("/");
                String propertiesFileName = paths[paths.length-1];

                File props = getFileByNameRecursievly(getProject().getProjectDir(), propertiesFileName);

                if (props == null || !props.exists()) {
                    log.trace("Resource {} not found, ignore it", str);
                    break;
                }
                try (InputStream stream = new FileInputStream(props)) {
                    log.info("Loading app properties from {}", str);
                    BOMInputStream bomInputStream = new BOMInputStream(stream);
                    try (Reader reader = new InputStreamReader(bomInputStream, StandardCharsets.UTF_8)) {
                        properties.load(reader);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Unable to read properties from stream", e);
                }
            }
        }
        return properties;
    }

    protected String getPropsConfigName() {
        // get properties from a set of app.properties files defined in web.xml
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        Document document;
        try {
            builder = factory.newDocumentBuilder();

            File webXmlFile = getFileByNameRecursievly(getProject().getProjectDir(), "web.xml");
            document = builder.parse(webXmlFile);
        } catch (SAXException | ParserConfigurationException | IOException e) {
            throw new RuntimeException("Can't get properties files names from core web.xml", e);
        }
        //document.getDocumentElement().normalize();
        NodeList nList = document.getElementsByTagName("context-param");
        for (int i = 0; i < nList.getLength(); i++) {
            Element nNode = (Element) nList.item(i);
            Node paramName = nNode.getElementsByTagName("param-name").item(0);
            String nodeValue = paramName.getTextContent();
            if ("appPropertiesConfig".equals(nodeValue)) {
                return nNode.getElementsByTagName("param-value").item(0).getTextContent();
            }
        }
        return null;
    }

    protected File getFileByNameRecursievly(File parentFile, String fileName) {
        Collection files = FileUtils.listFiles(parentFile, null, true);
        for (Object f : files) {
            File file = (File) f;
            if (file.getName().equals(fileName))
                return file;
        }
        return null;
    }

    protected String getScriptName(File file) {
        try {
            String dir = dbDir.getCanonicalPath();
            String path = file.getCanonicalPath();
            return path.substring(dir.length() + 1).replace("\\", "/");
        } catch (IOException e) {
            throw new GradleException("Exception while resolve script name", e);
        }
    }

    protected boolean isEmpty(String sql) {
        String[] lines = sql.split("[\r\n]+");
        for (String line : lines) {
            line = line.trim();
            if (!line.startsWith(SQL_COMMENT_PREFIX) && !StringUtils.isBlank(line)) {
                return false;
            }
        }
        return true;
    }

    protected void executeSqlScript(File file) {
        try {
            String script = FileUtils.readFileToString(file, "UTF-8");
            script = script.replaceAll("[\r\n]+", System.getProperty("line.separator"));

            Sql sql = getSql();

            ScriptSplitter splitter = new ScriptSplitter(delimiter);
            List<String> commands = splitter.split(script);
            for (String sqlCommand : commands) {
                if (!isEmpty(sqlCommand)) {
                    getProject().getLogger().info("[CubaDbTask] executing SQL: " + sqlCommand);
                    try {
                        sql.execute(sqlCommand);
                    } catch (SQLException e) {
                        throw new GradleException("Exception when executing SQL: " + sqlCommand, e);
                    }
                }
            }
        } catch (IOException e) {
            throw new GradleException("Exception when executing sql script: " + file.getAbsolutePath(), e);
        }
    }

    protected void markScript(String name, boolean init) {
        Sql sql = getSql();
        try {
            sql.executeUpdate("insert into SYS_DB_CHANGELOG (SCRIPT_NAME, IS_INIT) values (?, ?)",
                    Arrays.asList(name, (init ? 1 : 0)));
        } catch (SQLException e) {
            throw new GradleException("Exception when mark sql script", e);
        }
    }

    protected Sql getSql() {
        if (sqlInstance == null)
            try {
                sqlInstance = Sql.newInstance(dbUrl, dbUser, dbPassword, driver);
            } catch (SQLException | ClassNotFoundException e) {
                throw new GradleException("Unable to get SQL instance", e);
            }
        return sqlInstance;
    }

    protected void closeSql() {
        if (sqlInstance != null) {
            sqlInstance.close();
            sqlInstance = null;
        }
    }

    public static class ScriptFinder {

        protected String dbmsType;
        protected String dbmsVersion;
        protected File dbDir;
        protected List<String> extensions;
        private Project project;

        public ScriptFinder(String dbmsType, String dbmsVersion, File dbDir, List<String> extensions, Project project) {
            this.dbmsType = dbmsType;
            this.dbmsVersion = dbmsVersion;
            this.dbDir = dbDir;
            this.extensions = extensions;
            this.project = project;
        }

        public List<String> getModuleDirs() {
            if (!dbDir.exists()) {
                return Collections.emptyList();
            }
            String[] moduleDirs = dbDir.list();
            if (moduleDirs == null) {
                return Collections.emptyList();
            }
            List<String> sortedDirs = Arrays.asList(moduleDirs);
            sortedDirs.sort(Comparator.comparingLong(this::getModuleIndex)
                    .thenComparing(String::compareTo));
            return sortedDirs;
        }

        protected Long getModuleIndex(String moduleDir) {
            int dashIndex = moduleDir.indexOf('-');
            if (dashIndex > 1) {
                try {
                    return Long.valueOf(moduleDir.substring(0, dashIndex));
                } catch (NumberFormatException e) {
                    throw new GradleException(String.format("Invalid DB scripts directory name: %s", moduleDir));
                }
            } else
                throw new GradleException(String.format("Invalid DB scripts directory name: %s", moduleDir));
        }

        // Copy of com.haulmont.cuba.core.sys.DbUpdaterEngine#getUpdateScripts
        public List<File> getUpdateScripts(String oneModuleDir) {
            if (!dbDir.exists()) {
                return Collections.emptyList();
            }
            List<String> moduleDirs = getModuleDirs();
            List<File> databaseScripts = new ArrayList<>();
            for (String moduleDirName : moduleDirs) {
                if (StringUtils.isNotBlank(oneModuleDir) && !oneModuleDir.equals(moduleDirName)) {
                    continue;
                }

                File moduleDir = new File(dbDir, moduleDirName);
                File initDir = new File(moduleDir, "update");
                File scriptDir = new File(initDir, dbmsType);
                if (scriptDir.exists()) {
                    String[] extensionsArr = extensions.toArray(new String[extensions.size()]);
                    List<File> list = new ArrayList<>(FileUtils.listFiles(scriptDir, extensionsArr, true));
                    Map<File, File> file2dir = new HashMap<>();
                    list.forEach(file -> file2dir.put(file, scriptDir));

                    if (StringUtils.isNotBlank(dbmsVersion)) {
                        File optScriptDir = new File(initDir, dbmsType + "-" + dbmsVersion);
                        if (optScriptDir.exists()) {
                            Map<String, File> filesMap = new HashMap<>();
                            list.forEach(file -> filesMap.put(
                                    scriptDir.toPath().relativize(file.toPath()).toString(), file));
                            List<File> optList = new ArrayList<>(FileUtils.listFiles(optScriptDir, extensionsArr, true));
                            optList.forEach(file -> file2dir.put(file, optScriptDir));

                            Map<String, File> optFilesMap = new HashMap<>();
                            optList.forEach(file -> optFilesMap.put(
                                    optScriptDir.toPath().relativize(file.toPath()).toString(), file));

                            filesMap.putAll(optFilesMap);
                            list.clear();
                            list.addAll(filesMap.values());
                        }
                    }

                    list.sort((f1, f2) -> {
                        File f1Parent = f1.getAbsoluteFile().getParentFile();
                        File f2Parent = f2.getAbsoluteFile().getParentFile();
                        if (f1Parent.equals(f2Parent)) {
                            String f1Name = FilenameUtils.getBaseName(f1.getName());
                            String f2Name = FilenameUtils.getBaseName(f2.getName());
                            return f1Name.compareTo(f2Name);
                        }
                        File dir1 = file2dir.get(f1);
                        File dir2 = file2dir.get(f2);
                        Path p1 = dir1.toPath().relativize(f1.toPath());
                        Path p2 = dir2.toPath().relativize(f2.toPath());
                        return p1.compareTo(p2);
                    });

                    databaseScripts.addAll(list);
                }
            }

            return databaseScripts;
        }

        public List<File> getInitScripts(String oneModuleDir) {
            if (!dbDir.exists()) {
                logInfo("[CubaDbTask] [getInitScripts] " + dbDir + " doesn't exist");
                return Collections.emptyList();
            }
            List<String> moduleDirs = getModuleDirs();
            List<File> files = new ArrayList<>();
            logInfo("[CubaDbTask] [getInitScripts] modules: [" + StringUtils.join(moduleDirs, ", ") + "]");
            for (String moduleDirName : moduleDirs) {
                if (StringUtils.isNotBlank(oneModuleDir) && !oneModuleDir.equals(moduleDirName)) {
                    continue;
                }
                File moduleDir = new File(dbDir, moduleDirName);
                File initDir = new File(moduleDir, "init");
                File scriptDir = new File(initDir, dbmsType);
                if (!scriptDir.exists()) {
                    logInfo("[CubaDbTask] [getInitScripts] " + scriptDir + " doesn't exist");
                    continue;
                }
                FilenameFilter filenameFilter = (dir, name) -> name.endsWith("create-db.sql");
                File[] scriptFiles = scriptDir.listFiles(filenameFilter);
                if (scriptFiles == null) {
                    scriptFiles = new File[]{};
                }
                List<File> list = new ArrayList<>(Arrays.asList(scriptFiles));
                if (StringUtils.isNotBlank(dbmsVersion)) {
                    File optScriptDir = new File(initDir, dbmsType + "-" + dbmsVersion);
                    if (optScriptDir.exists()) {
                        File[] optFiles = optScriptDir.listFiles(filenameFilter);
                        if (optFiles == null) {
                            optFiles = new File[]{};
                        }

                        Map<String, File> filesMap = new HashMap<>();
                        for (File scriptFile : scriptFiles) {
                            filesMap.put(scriptDir.toPath().relativize(scriptFile.toPath()).toString(), scriptFile);
                        }

                        Map<String, File> optFilesMap = new HashMap<>();
                        for (File optFile : optFiles) {
                            optFilesMap.put(optScriptDir.toPath().relativize(optFile.toPath()).toString(), optFile);
                        }

                        filesMap.putAll(optFilesMap);
                        list.clear();
                        list.addAll(filesMap.values());
                    }
                }

                list.sort(Comparator.comparing(File::getName));

                logInfo("[CubaDbTask] [getInitScripts] files: " + list);
                files.addAll(list);
            }
            return files;
        }

        private void logInfo(String msg) {
            if (project != null) {
                project.getLogger().info(msg);
            }
        }
    }

    protected Map<String, String> parseDatabaseParams(String connectionParams) {
        Map<String, String> result = new HashMap<>();
        if (connectionParams.startsWith("?")) {
            connectionParams = connectionParams.substring(1);
        }
        for (String param : connectionParams.split("[&,;]")) {
            int index = param.indexOf('=');
            if (index > 0) {
                String key = param.substring(0, index);
                String value = param.substring(index + 1);
                if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                    result.put(key.trim(), value.trim());
                }
            }
        }
        return result;
    }

    protected String cleanSchemaName(String schemaName) {
        return StringUtils.isNotEmpty(schemaName) ? schemaName.replace("\"", "") : schemaName;
    }
}