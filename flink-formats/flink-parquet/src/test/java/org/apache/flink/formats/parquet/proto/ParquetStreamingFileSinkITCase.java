package org.apache.flink.formats.parquet.proto;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.generated.HelloWorldOuterClass.EventBase;
import org.apache.flink.formats.parquet.generated.HelloWorldOuterClass.HelloWorld;
import org.apache.flink.formats.parquet.generated.HelloWorldOuterClass.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.test.util.AbstractTestBase;

import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoParquetReader;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Simple integration test case for writing bulk encoded files with the
 * {@link StreamingFileSink} with Parquet.
 */
@SuppressWarnings("serial")
public class ParquetStreamingFileSinkITCase extends AbstractTestBase {


	private static <T> void validateResults(File folder, List<T> expected) throws Exception {
		File[] buckets = folder.listFiles();
		assertNotNull(buckets);
		assertEquals(1, buckets.length);

		File[] partFiles = buckets[0].listFiles();
		assertNotNull(partFiles);
		assertEquals(2, partFiles.length);

		for (File partFile : partFiles) {
			assertTrue(partFile.length() > 0);

			final List<MessageOrBuilder> fileContent = readParquetFile(partFile);
			// assertEquals(expected, fileContent);
//			assertThat("List equality without order",
//				fileContent, containsInAnyOrder(expected));
		}
	}

	private static <T extends Builder> List<MessageOrBuilder> readParquetFile(File file) throws Exception {
		ParquetReader<T> reader = ProtoParquetReader.<T>builder(new org.apache.hadoop.fs.Path(file.toURI())).build();
		ArrayList<MessageOrBuilder> results = new ArrayList<>();
		T next;
		while ((next = reader.read()) != null) {

			MessageOrBuilder msgClone = next.clone();
			results.add(msgClone);
			System.out.println("Element is" + msgClone.toString());
		}
		System.out.println("Element is" + results.toString());
		return results;
	}

	@Test
	public void testWriteParquetProtoSpecific() throws Exception {

		final File folder = new File("/Users/kailashdayanand/kdayanand/dir");

		HelloWorld message1 =
			HelloWorld.newBuilder()
				.setEventBase(
					EventBase.newBuilder()
						.setEventId(UUID.newBuilder().setValue("2121").build())
						.setOccurredAt(
							(System.currentTimeMillis() - 10000) / 1000))
				.build();

		HelloWorld message2 =
			HelloWorld.newBuilder()
				.setEventBase(
					EventBase.newBuilder()
						.setEventId(UUID.newBuilder().setValue("1213").build())
						.setOccurredAt(
							System.currentTimeMillis() / 1000))
				.build();

		HelloWorld message3 =
			HelloWorld.newBuilder()
				.setEventBase(
					EventBase.newBuilder()
						.setEventId(UUID.newBuilder().setValue("2313").build())
						.setOccurredAt(
							(System.currentTimeMillis() + 10000) / 1000))
				.build();

		final List<HelloWorld> data = new ArrayList<>();
		data.add(message1);
		data.add(message2);
		data.add(message3);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(100);

		DataStream<HelloWorld> stream = env.addSource(
			new FiniteTestSource<>(data), TypeInformation.of(HelloWorld.class));

		stream.addSink(
			StreamingFileSink.forBulkFormat(
				Path.fromLocalFile(folder),
				ParquetProtoWriters.forType(HelloWorld.class))
				.build());

		env.execute();

		validateResults(folder, data);
	}

	// ------------------------------------------------------------------------

//	@Test
//	public void testWriteParquetAvroGeneric() throws Exception {
//
//		final File folder = TEMPORARY_FOLDER.newFolder();
//
//		final Schema schema = Address.getClassSchema();
//
//		final Collection<GenericRecord> data = new org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.GenericTestDataCollection();
//
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(1);
//		env.enableCheckpointing(100);
//
//		DataStream<GenericRecord> stream = env.addSource(
//			new FiniteTestSource<>(data), new GenericRecordAvroTypeInfo(schema));
//
//		stream.addSink(
//			StreamingFileSink.forBulkFormat(
//				Path.fromLocalFile(folder),
//				ParquetAvroWriters.forGenericRecord(schema))
//				.build());
//
//		env.execute();
//
//		List<Address> expected = Arrays.asList(
//			new Address(1, "a", "b", "c", "12345"),
//			new Address(2, "x", "y", "z", "98765"));
//
//		validateResults(folder, SpecificData.get(), expected);
//	}

//	@Test
//	public void testWriteParquetAvroReflect() throws Exception {
//
//		final File folder = TEMPORARY_FOLDER.newFolder();
//
//		final List<org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum> data = Arrays.asList(
//			new org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum("a", 1), new org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum("b", 2), new org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum("c", 3));
//
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setParallelism(1);
//		env.enableCheckpointing(100);
//
//		DataStream<org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum> stream = env.addSource(
//			new FiniteTestSource<>(data), TypeInformation.of(org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum.class));
//
//		stream.addSink(
//			StreamingFileSink.forBulkFormat(
//				Path.fromLocalFile(folder),
//				ParquetAvroWriters.forReflectRecord(org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum.class))
//				.build());
//
//		env.execute();
//
//		validateResults(folder, ReflectData.get(), data);
//	}

//	private static class GenericTestDataCollection extends AbstractCollection<GenericRecord> implements Serializable {
//
//		@Override
//		public Iterator<GenericRecord> iterator() {
//			final GenericRecord rec1 = new GenericData.Record(Address.getClassSchema());
//			rec1.put(0, 1);
//			rec1.put(1, "a");
//			rec1.put(2, "b");
//			rec1.put(3, "c");
//			rec1.put(4, "12345");
//
//			final GenericRecord rec2 = new GenericData.Record(Address.getClassSchema());
//			rec2.put(0, 2);
//			rec2.put(1, "x");
//			rec2.put(2, "y");
//			rec2.put(3, "z");
//			rec2.put(4, "98765");
//
//			return Arrays.asList(rec1, rec2).iterator();
//		}
//
//		@Override
//		public int size() {
//			return 2;
//		}
//	}
//
//	// ------------------------------------------------------------------------
//
//	/**
//	 * Test datum.
//	 */
//	public static class Datum implements Serializable {
//
//		public String a;
//		public int b;
//
//		public Datum() {
//		}
//
//		public Datum(String a, int b) {
//			this.a = a;
//			this.b = b;
//		}
//
//		@Override
//		public boolean equals(Object o) {
//			if (this == o) {
//				return true;
//			}
//			if (o == null || getClass() != o.getClass()) {
//				return false;
//			}
//
//			org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum datum = (org.apache.flink.formats.parquet.avro.ParquetStreamingFileSinkITCase.Datum) o;
//			return b == datum.b && (a != null ? a.equals(datum.a) : datum.a == null);
//		}
//
//		@Override
//		public int hashCode() {
//			int result = a != null ? a.hashCode() : 0;
//			result = 31 * result + b;
//			return result;
//		}
//	}
}

