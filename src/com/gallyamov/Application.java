package com.gallyamov;

import org.apache.commons.cli.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Пробегает по директории с тайлами и пишет их в БД.
 */
public class Application {
	private static String source;
	private static String databaseFile;
	private static String tableName;

	private static int threadsCount;
	private static int batchSize;

	private static Connection connection;

	private static ExecutorService pool;
	private static ArrayList<File> taskFiles = new ArrayList<>();

	private static long count = 0;

	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException,
			ClassNotFoundException, IOException, ParseException {
		try {
			parseCommandArgs(args);

			if (source == null && databaseFile == null || tableName == null) {
				throw new IllegalArgumentException("wrong arguments, be sure to pass source, database, table");
			}

			File file = new File(databaseFile);
			if (file.exists()) {
				if (!file.delete()) {
					throw new IllegalStateException("Failed to remove file");
				}
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}

		pool = Executors.newFixedThreadPool(threadsCount);

		System.out.println(String.format("Start with %d threads and batch size %d", threadsCount, batchSize));
		Instant start = Instant.now();

		connect();
		createTable();

		System.out.println("adding ...");

		connection.setAutoCommit(false);
		walk(new File(source));
		if (taskFiles.size() > 0) {
			insertAndWait();
		}
		connection.setAutoCommit(true);

		System.out.println("indexing ...");
		createIndex();

		close();

		Instant end = Instant.now();

		System.out.println("done");
		System.out.println("total count: " + count);
		System.out.println(String.format("execution time: %s", Duration.between(start, end)));

		System.exit(0);
	}

	private static void walk(File dir) throws SQLException, IOException, ExecutionException, InterruptedException {
		File[] files = dir.listFiles();

		if (files == null) {
			return;
		}

		for (final File file : files) {
			if (file.isDirectory()) {
				walk(file);
			} else {
				taskFiles.add(file);
			}

			if (taskFiles.size() >= batchSize) {
				insertAndWait();
			}
		}
	}

	private static void insertAndWait() throws SQLException, IOException, ExecutionException, InterruptedException {
		ArrayList<Future<Record>> tasks = new ArrayList<>();

		for (final File file : taskFiles) {
			tasks.add(pool.submit(new Callable<Record>() {
				@Override
				public Record call() throws Exception {
					return makeRecord(file);
				}
			}));
		}

		String sql = "INSERT INTO " + tableName + " (z, x, y, data) VALUES (?, ?, ?, ?);";
		final PreparedStatement stm = connection.prepareStatement(sql);

		for (Future<Record> task : tasks) {
			Record record = task.get();

			if (record != null) {
				stm.setInt(1, record.z);
				stm.setInt(2, record.x);
				stm.setInt(3, record.y);
				if (record.data.length > 0) {
					stm.setBytes(4, record.data);
				}

				stm.addBatch();
			}
		}

		stm.executeBatch();
		stm.close();
		connection.commit();

		count += taskFiles.size();
		System.out.println("count: " + count);

		taskFiles.clear();
	}

	public static Record makeRecord(File file) throws SQLException, IOException {
		String fileName = file.getName();
		String ext = file.getName().substring(fileName.lastIndexOf(".") + 1);

		if (!ext.equals("png")) {
			return null;
		}

		Pattern p = Pattern.compile("(\\d+)/(\\d+)/(\\d+)\\." + ext);
		Matcher m = p.matcher(file.getAbsolutePath());

		if (!m.find()) {
			return null;
		}

		int z = Integer.valueOf(m.group(1));
		int x = Integer.valueOf(m.group(2));
		int y = Integer.valueOf(m.group(3));
		byte[] data;

		try (FileInputStream is = new FileInputStream(file); BufferedInputStream bis = new BufferedInputStream(is)) {
			data = new byte[(int) file.length()];  // setBlob, setBinaryStream не работает
			bis.read(data);
		}

		return new Record(z, x, y, data);
	}

	private static void parseCommandArgs(String args[]) throws ParseException {
		Options options = new Options()
				.addOption("s", "source", true, "directory of tiles")
				.addOption("d", "database", true, "database name")
				.addOption("t", "table", true, "table name")
				.addOption("n", "thread", true, "thread count")
				.addOption("b", "batch", true, "batch of files size");

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);

		if (cmd.hasOption("source")) {
			source = cmd.getOptionValue("source", null);
		}

		if (cmd.hasOption("database")) {
			databaseFile = cmd.getOptionValue("database", null);
		}

		if (cmd.hasOption("table")) {
			tableName = cmd.getOptionValue("table", null);
		}

		int cores = Runtime.getRuntime().availableProcessors();
		threadsCount = Integer.valueOf(cmd.getOptionValue("thread", String.valueOf(cores)));
		batchSize = Integer.valueOf(cmd.getOptionValue("batch", String.valueOf(cores * 256)));
	}

	private static void connect() throws SQLException, ClassNotFoundException {
		Class.forName("org.sqlite.JDBC");
		connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile);
	}

	private static void close() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	private static void createTable() throws SQLException {
		String dropIndexSql = "DROP TABLE IF EXISTS " + tableName;

		String createTableSql = "CREATE TABLE " + tableName + " (" +
				"z INTEGER NOT NULL, " +
				"x INTEGER NOT NULL, " +
				"y INTEGER NOT NULL, " +
				"data BLOB" +
				")";

		Statement stm = connection.createStatement();
		stm.executeUpdate(dropIndexSql);
		stm.executeUpdate(createTableSql);
		stm.close();
	}

	public static void createIndex() throws SQLException {
		String sql = "CREATE INDEX zxy ON " + tableName + " (z, x, y)";

		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
	}

	private static class Record {
		private int x, y, z;
		private byte[] data;

		public Record(int z, int x, int y, byte[] data) {
			this.x = x;
			this.y = y;
			this.z = z;
			this.data = data;
		}
	}
}
