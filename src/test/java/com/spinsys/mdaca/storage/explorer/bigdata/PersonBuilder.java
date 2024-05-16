package com.spinsys.mdaca.storage.explorer.bigdata;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;

/**
 * This class provides some useful utilities for constructing
 * test objects for use by its subclasses.
 */
public class PersonBuilder {

	private static Random random = new Random();
	
	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.PersonBasedIT");


	public static class FlatPerson implements Serializable {
		private static final long serialVersionUID = 1L;
		protected String name;	    
	    protected byte idB = (byte) random.nextInt(127);
	    protected long ssn = random.nextInt(999_999_999);
	    protected float weight = (float) random.nextDouble() * 400;
	    protected double code1 = random.nextDouble();
	    BigInteger numWishListIds = new BigInteger("111222333444555666777888999");
	    BigDecimal aBigDec = new BigDecimal(numWishListIds, 10);
	    protected short areaCodeS = (short) random.nextInt(999);
	    protected int phoneI = random.nextInt(9_999_999);

		public String getName() {
			return name;
		}
		public void setName(String body) {
			this.name = body;
		}
		public byte getIdB() {
			return idB;
		}
		public void setIdB(byte idB) {
			this.idB = idB;
		}
		public long getSsn() {
			return ssn;
		}
		public void setSsn(long ssn) {
			this.ssn = ssn;
		}
		public float getWeight() {
			return weight;
		}
		public void setWeight(float weight) {
			this.weight = weight;
		}
		public double getCode1() {
			return code1;
		}
		public void setCode1(double code1) {
			this.code1 = code1;
		}
		public BigInteger getNumWishListIds() {
			return numWishListIds;
		}
		public void setNumWishListIds(BigInteger numWishListIds) {
			this.numWishListIds = numWishListIds;
		}
		public BigDecimal getaBigDec() {
			return aBigDec;
		}
		public void setaBigDec(BigDecimal aBigDec) {
			this.aBigDec = aBigDec;
		}
		public short getAreaCodeS() {
			return areaCodeS;
		}
		public void setAreaCodeS(short areaCodeS) {
			this.areaCodeS = areaCodeS;
		}
		public int getPhoneI() {
			return phoneI;
		}
		public void setPhoneI(int phoneI) {
			this.phoneI = phoneI;
		}
	}
	
	public static class Person extends NonPrimitivePerson {
		private static final long serialVersionUID = 1L;
		NumericData numericData = new NumericData();
	    
		public NumericData getNumericData() {
			return numericData;
		}
		public void numericData(NumericData idNumeric) {
			this.numericData = idNumeric;
		}
	}
	
	/**
	 * Has a primitive-valued field and a field with a Map type
	 */
	public static class PersonWithTraits implements Serializable {
		protected static final long serialVersionUID = 1L;
		protected String name;
	    HashMap<String, String> miscTraits = new HashMap<>();
	    
		public String getName() {
			return name;
		}
		public void setName(String body) {
			this.name = body;
		}
		public HashMap<String, String> getMiscTraits() {
			return miscTraits;
		}
		public void setMiscTraits(HashMap<String, String> miscTraits) {
			this.miscTraits = miscTraits;
		}
	}
	
	/**
	 * Has a primitive-valued field and a field with a Map type
	 */
	public static class NonPrimitivePerson extends PersonWithTraits {
		private static final long serialVersionUID = 1L;
		
		ArrayList<Integer> randomNumbers = new ArrayList<>();
		Phone homePhone = new Phone();
		
		public ArrayList<Integer> getRandomNumbers() {
			return randomNumbers;
		}
		public void setRandomNumbers(ArrayList<Integer> randomNumbers) {
			this.randomNumbers = randomNumbers;
		}
		public Phone getHomePhone() {
			return homePhone;
		}
		public void setHomePhone(Phone homePhone) {
			this.homePhone = homePhone;
		}
	    
	}
	
	public static class NumericData implements Serializable {
		private static final long serialVersionUID = 1L;
	    protected byte idB = (byte) random.nextInt(127);
	    protected long ssn = random.nextInt(999_999_999);
	    protected float weight = (float) random.nextDouble() * 400;
	    protected double code1 = random.nextDouble();
	    ArrayList<Phone> phones = new ArrayList<>();
	    int[] twoMoreIds = { random.nextInt(999_999), random.nextInt(999_999_999) };
	    BigInteger numWishListIds = new BigInteger("111222333444555666777888999");
	    BigDecimal aBigDec = new BigDecimal(numWishListIds, 10);

	    public NumericData() {
	    	for (int i = 0; i < random.nextInt(4); i++) {
		    	phones.add(new Phone());
	    	}
	    }
	    public byte getIdB() {
			return idB;
		}
		public void setIdB(byte idB) {
			this.idB = idB;
		}
		public long getSsn() {
			return ssn;
		}
		public void setSsn(long ssn) {
			this.ssn = ssn;
		}
		public float getWeight() {
			return weight;
		}
		public void setWeight(float weight) {
			this.weight = weight;
		}
		public double getCode1() {
			return code1;
		}
		public void setCode1(double code1) {
			this.code1 = code1;
		}
		public ArrayList<Phone> getPhones() {
			return phones;
		}
		public void setPhones(ArrayList<Phone> phones) {
			this.phones = phones;
		}
		public int[] getTwoMoreIds() {
			return twoMoreIds;
		}
		public void setTwoMoreIds(int[] twoMoreIds) {
			this.twoMoreIds = twoMoreIds;
		}
		public BigInteger getNumWishListIds() {
			return numWishListIds;
		}
		public void setNumWishListIds(BigInteger numWishListIds) {
			this.numWishListIds = numWishListIds;
		}
		public BigDecimal getaBigDec() {
			return aBigDec;
		}
		public void setaBigDec(BigDecimal aBigDec) {
			this.aBigDec = aBigDec;
		}
	}

	public static class Phone implements Serializable {
		private static final long serialVersionUID = 1L;
	    private short areaCodeS = (short) random.nextInt(999);
	    private int phoneI = random.nextInt(9_999_999);
		public short getAreaCodeS() {
			return areaCodeS;
		}
		public void setAreaCodeS(short areaCodeS) {
			this.areaCodeS = areaCodeS;
		}
		public int getPhoneI() {
			return phoneI;
		}
		public void setPhoneI(int phoneI) {
			this.phoneI = phoneI;
		}
	}


	/**
	 * Generate an AVRO file based on the ExportData class
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	// see https://stackoverflow.com/questions/11866466/using-apache-avro-reflect
	Schema writeAVRO(String fileName) throws IOException {
		Schema schema = ReflectData.get().getSchema(Person.class);
		File file = new File(fileName);
		DatumWriter<Person> writer = null;
		DataFileWriter<Person> dataFileWriter = null;
		try {
			writer = new ReflectDatumWriter<>(Person.class);
			dataFileWriter = new DataFileWriter<>(writer);
			
			dataFileWriter.setCodec(CodecFactory.deflateCodec(9))
		      .create(schema, file);

		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	Person person = createPersonWithId(i);
		    	person.getMiscTraits().put("numberOfToes", "" + i);
				dataFileWriter.append(person);
		    }
		}
		finally {
			if (dataFileWriter != null) {
				dataFileWriter.close();
			}
		}
		logger.info("schema =\n" + schema);
		return schema;
	}

	FlatPerson createFlatPersonWithId(byte id) throws IOException {
		FlatPerson person = new FlatPerson();
		person.setName("fay" + id);
		person.setIdB(id);
		return person;
	}
	
	PersonWithTraits createPersonWithTraits(byte id) throws IOException {
		PersonWithTraits person = new PersonWithTraits();
		person.setName("tom" + id);
		person.getMiscTraits().put("lastUpdated", (new Date()).toString());
		person.getMiscTraits().put("today", FileUtil.getTimestamp());
		return person;
	}
	
	Person createPerson(int id) throws IOException {
		Person person = new Person();
		person.setName("Connie" + id);
		person.getMiscTraits().put("lastUpdated", (new Date()).toString());
		person.getMiscTraits().put("today", FileUtil.getTimestamp());
		ArrayList<Integer> randomNumbers = person.getRandomNumbers();
		
		for (int i = 0; i < random.nextInt(4); i++) {
			randomNumbers.add(random.nextInt(999));
		}
		return person;
	}
	
	Person createPersonWithId(byte id) throws IOException {
		Person person = new Person();
		person.setName("bob" + id);
		person.getNumericData().setIdB(id);
		person.getNumericData().getPhones().add(new Phone());
		person.getNumericData().getPhones().add(new Phone());
		person.getMiscTraits().put("lastUpdated", (new Date()).toString());
		person.getMiscTraits().put("today", FileUtil.getTimestamp());
		return person;
	}
	
}
