package com.ar6.ng7m;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class AmatCleaner
{
	public AmatCleaner()
	{
	}

	// local project objects
	private CmdLineArgs _cmdLineArgs = null;
	final private String _generationTimeStamp = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
	private static final Pattern asciiPattern = Pattern.compile("[^\\p{ASCII}]");


	public boolean Execute()
	{
		double startTime = System.currentTimeMillis();
		boolean bSuccessful = false;
		CmdLineArgs argumentValues = getConfiguration();
		String outputDestinationPath = argumentValues.GetOutputDestination();
		File outputDirectory = outputDirectoryIsValid(outputDestinationPath);
		String workingDirectoryPath = argumentValues.GetWorkingDirectory();
		File workingDirectory = workingDirectoryIsValid(workingDirectoryPath);

		if(!(null == outputDirectory || null == workingDirectory))
		{
			if(argumentValues.readOnly)
			{
				out("Readonly enabled. New output directory files will not be created.");
			}
			else
			{
				out("File output enabled at this location: " + outputDestinationPath);
			}

			if(argumentValues.verboseOutput)
			{
				out("Verbose console output enabled...");
			}

			// get the fcc uls complete zip data
			if(argumentValues.downloadFccZip)
			{
				String zipFileName = argumentValues.GetZipFileName();

				// create the temp working directory if needed

				Path filePath = Paths.get(workingDirectoryPath, zipFileName);

                // reach out and download the file, make sure and set proxy jvm properties if
                // running from a NAT based network, user must set env var proxy variables
                out("Downloading: " + argumentValues.fccAmatuerLicenseCompleteURL);
                out("Be Patient! File is over 100 megs...");

                if (CopyFTPURLtoFile(argumentValues.fccAmatuerLicenseCompleteURL, filePath.toString()))
                {
                    // if we made it here, we have a zip file that needs to be extracted
                    unZipFile(workingDirectoryPath, zipFileName);

                    bSuccessful = true;
                }
			}
			else
			{
				// show success to reuse already downloaded files for testing
				bSuccessful = true;
			}
			if(bSuccessful)
			{
				out("Begin File Processing...");
					if (workingDirectory.isDirectory())
					{
						// copy everything to destination
						try
						{
							if (!argumentValues.readOnly)
							{
								out("Copying source directory to output directory...");
								FileUtils.copyDirectory(workingDirectory, outputDirectory);
							}
						} catch (IOException e)
						{
							out("Error: " + e.getCause());
						}

						FilenameFilter filter = new FilenameFilter()
						{
							@Override
							public boolean accept(File dir, String name) {
								return name.toLowerCase().endsWith(".dat");
							}
						};

						File[] filesInDir = workingDirectory.listFiles(filter);

						// rip through the files that match the file name filter object
						for (File file : filesInDir)
						{
							parse(file.getAbsolutePath(), file.getName(), outputDestinationPath);
						}

						if (argumentValues.createOutputZipFile)
						{
							Path path = Paths.get(outputDestinationPath, argumentValues.GetZipFileName());

							createZipFile(outputDestinationPath, path.toAbsolutePath().toString());
						}
					}
				}

				// check to see if we need to create the N1MM call history file after files have been cleaned
				if (argumentValues.createN1MMCallHistory)
				{
					if (bSuccessful)
					{
						// check and see if we need to download the VE callsign database
						if (argumentValues.includeVECallHistory)
						{
							String veWorkingingDirectoryPath = argumentValues.GetVEN1MMCallHistoryWorkingDirectory();
							File veWorkingDirectory = workingDirectoryIsValid(workingDirectoryPath);

							if (veWorkingDirectory.isDirectory())
							{
								if (argumentValues.downloadVEZip)
								{
									String veZipFileName = argumentValues.GetVeZipFileName();

									// create the temp working directory if needed
									Path filePath = Paths.get(veWorkingingDirectoryPath, veZipFileName);

									// reach out and download the file, make sure and set proxy jvm properties if
									// running from a NAT based network, user must set env var proxy variables
									out("Downloading: " + argumentValues.veAmatuerLicenseCompleteURL);
									out("Be Patient! File is over 2 megs...");

									if (CopyHTTPURLtoFile(argumentValues.veAmatuerLicenseCompleteURL, filePath.toString()))
									{
										// if we made it here, we have a zip file that needs to be extracted
										unZipFile(veWorkingingDirectoryPath, veZipFileName);
									}
								}
							}
						}	// if including ve call database

						bSuccessful = CreateN1MMCallHistory();
					}
				}	// if creating n1mm call hisotry

				out("Finished Processing...");
				out("Processing Completed in: " + (System.currentTimeMillis() - startTime) / 1000 + " Seconds...");
			}

		out("Successful Processing = " + bSuccessful);
	    out("Exiting...");

		return bSuccessful;
	}

	private boolean parse(String absoluteFile, String fileName, String outputDestinationPath)
	{
		BufferedReader reader = null;
		BufferedWriter writer = null;
		String fileToProcess, record;
		String splitBy = Pattern.quote("|");
		int recordNum = 0;
		String headerFooter = "========================================================================================";
		int numErrors = 0;

		out(headerFooter);
		out("Processing File: " + absoluteFile);
		out(headerFooter);

		try
		{
			reader = new BufferedReader(new FileReader(absoluteFile));

			// now make sure we can create the output file
			Path path = Paths.get(outputDestinationPath, fileName);

			if(!getConfiguration().readOnly)
			{
				writer = new BufferedWriter(new FileWriter(path.toString()));
			}

			if(null != writer)
			{
				while ((record = reader.readLine()) != null)
				{
					++recordNum;
					StringBuilder sbRecord = new StringBuilder(record);

					if (!validateFields(fileName, recordNum, sbRecord))
					{
						++numErrors;
						out("Error found in record: " + recordNum + " with value: " + "\"" + record + "\"");
					} else
					{
						// assume record could have been cleaned up and reassign record to sbRecord
						record = sbRecord.toString();
							writer.write(record);
							writer.newLine();
					}
				}
			}
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		} catch(IOException e)
		{
			e.printStackTrace();
		} finally
		{
			if(reader != null)
			{
				try
				{
					reader.close();
				} catch(IOException e)
				{
					e.printStackTrace();
				}
			}
			if(writer != null)
			{
				try
				{
					writer.close();
				} catch(IOException e)
				{
					e.printStackTrace();
				}
			}
		}

		out("File : " + absoluteFile + " Finished Processing with: " + recordNum + " rows. Records removed due to errors: " + numErrors);
		out(headerFooter);
		out("");
		return (numErrors > 0) ? true : false;
	}

	private boolean CreateN1MMCallHistory()
	{
		CmdLineArgs argumentValues = getConfiguration();
		String outputDestinationPath = argumentValues.GetOutputDestination();
		String n1mmCallHistoryOutputDirectory = argumentValues.GetN1MMCallHistoryOutputDirectory();

		File HDdatFile = new File(Paths.get(outputDestinationPath,"HD.dat").toString());
		File AMdatFile = new File(Paths.get(outputDestinationPath,"AM.dat").toString());
		File ENdatFile = new File(Paths.get(outputDestinationPath,"EN.dat").toString());
		BufferedReader reader = null;
		BufferedWriter writer = null;
		String record;
		String splitBy = Pattern.quote("|");
		String headerFooter = "========================================================================================";
		String n1mmCallHistoryFileName = GetN1MMPotaCallHisotryFileName();
		String[] fields;
		String uniqueSystemIdentifier, licenseStatus, licenseClass, attentionLine;
		String callSign, firstName, state, applicantTypeCode, expiredDate;
		String callHistoryRecord;
		int notActiveCount = 0, activeButExpired = 0;
		ArrayList<String> workingArrayList;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy");
		Date workingDate;
		Calendar calendar = Calendar.getInstance();
		Date today = new Date();
		boolean licenseExpired;
		List<String> bigListOfCallsigns = new ArrayList<>();
		List<String> bigListOfVECallsigns = new ArrayList<>();

		out(headerFooter);
		out("Creating N1MM call history: " + GetDateTimeUTC() + " File: " + n1mmCallHistoryFileName);
		out(headerFooter);

		try
		{
			// see file:///C:/Users/ng7m/Downloads/public_access_database_definitions_20240215.pdf for FCC file format details
			// read in the HD.dat file and create a hash table used to look up active and expired
			HashMap<String, ArrayList<String>> hdHashMap = new HashMap<>();

			reader = new BufferedReader(new FileReader((HDdatFile)));
			while ((record = reader.readLine()) != null)
			{
				ArrayList<String> hdEntryList = new ArrayList<>();

				fields = record.split(splitBy, -1);

				uniqueSystemIdentifier = fields[1];
				callSign = fields[4];
				hdEntryList.add(callSign);
				licenseStatus = fields[5];
				hdEntryList.add(licenseStatus);
				expiredDate = fields[8];
				hdEntryList.add(expiredDate);

				hdHashMap.put(uniqueSystemIdentifier, hdEntryList);
			}

			try
			{
				reader.close();
			} catch(IOException e)
			{
				e.printStackTrace();
			}

			// read in the AM.dat file and create a hash table used to look up the license class
			HashMap<String, String> amHashMap = new HashMap<>();
			int generalClassCount = 0, advancedClassCount= 0, extraClassCount = 0;
			int clubCalls = 0, totalRecords = 0;
			int allOtherLicenceClasses = 0;
			int veTotalRecordsIncluded = 0, veBasicLicensesExcluded = 0, veClubCalls = 0, veTotalRawRecordsProcessed = 0;

			reader = new BufferedReader(new FileReader((AMdatFile)));
			while ((record = reader.readLine()) != null)
			{

				fields = record.split(splitBy, -1);
				uniqueSystemIdentifier = fields[1];
				callSign = fields[4];
				licenseClass = fields[5];

				amHashMap.put(uniqueSystemIdentifier, licenseClass);

			}

			try
			{
				reader.close();
			} catch(IOException e)
			{
				e.printStackTrace();
			}

			reader = new BufferedReader(new FileReader(ENdatFile));

			// now make sure we can create the output file
			Path path = Paths.get(n1mmCallHistoryOutputDirectory, n1mmCallHistoryFileName);

			if(!getConfiguration().readOnly)
			{
				writer = new BufferedWriter(new FileWriter(path.toString()));
			}
			if (null != writer)
			{
				boolean addRecord = false;

				while ((record = reader.readLine()) != null)
				{
					totalRecords++;
					fields = record.split(splitBy, -1);
					uniqueSystemIdentifier = fields[1];
					callSign = fields[4].toUpperCase();
					firstName = fields[8];
					state = fields[17].toUpperCase();
					attentionLine = fields[20]; // used to get a name for club calls
					applicantTypeCode = fields[23].toUpperCase();

					// check license status from the HD.had hash map and only continue if 'A' Active status
					workingArrayList = hdHashMap.get(uniqueSystemIdentifier);
					licenseStatus = workingArrayList.get(1);
					expiredDate = workingArrayList.get(2);

					if (licenseStatus.equals("A")) // only include active
					{
						// check amHashMap to see if we have a general, advanced or extra class license
						licenseClass = amHashMap.get(uniqueSystemIdentifier);

						// check if the call is still active in the expiration windows
						licenseExpired = false;
						if (licenseClass.equals("G") ||
								licenseClass.equals("A") ||
								licenseClass.equals("E") ||
                                licenseClass.isEmpty())
						{
							if (!expiredDate.isEmpty())
							{
								workingDate = dateFormat.parse(expiredDate);
								calendar.setTime(workingDate);
								calendar.add(Calendar.DATE, -14);
								workingDate = calendar.getTime();

								licenseExpired = today.after(workingDate);
								if (licenseExpired)
								{
									//out("Callsign: " + callSign + " License Expiration: " + workingDate.toString() + " License status: " + licenseStatus);
									activeButExpired++;
									continue;
								}
							}
						}

						if (null != licenseClass)
						{
							switch (licenseClass)
							{
								case "G":
									generalClassCount++;
									addRecord = true;
									break;
								case "A":
									advancedClassCount++;
									addRecord = true;
									break;
								case "E":
									extraClassCount++;
									addRecord = true;
									break;
								case "":  // club calls?  Get the name from attentionLine
									if (applicantTypeCode.equals("B")) // B is a Club Call, see https://www.fcc.gov/sites/default/files/pubacc_tbl_abbr_names_20240215.pdf
									{
										clubCalls++;

										firstName = ScrubFirstName(attentionLine);
										addRecord = true;
									}
									else
									{
										addRecord = false;
									}
									break;
								default:
									// exclude anything else
									allOtherLicenceClasses++;
									addRecord = false;
							}

							if (addRecord)
							{
								// build up an array of the strings to export / write to the file so we can get
								// stats to put in comments at the top of the call history file

								callHistoryRecord = callSign + ',' + ScrubFirstName(firstName) + ',' + state + '\r' + '\n';
								bigListOfCallsigns.add(callHistoryRecord);
							}
						}
					}  // end of if active callsign
					else
					{
						notActiveCount++;
					}
				} // while ripping through EN.dat records

				// now check to see if we loaded the VE callsign database and rip through it
				if (argumentValues.includeVECallHistory)
				{
					String veN1MMCallHistoryWorkingDirectory = argumentValues.GetVEN1MMCallHistoryWorkingDirectory();
					String providence, qualA, qualB, qualC, qualD, qualE, clubName1, clubName2, clubProvidence;

					File veAmateurDelimitedTxtFile = new File(Paths.get(veN1MMCallHistoryWorkingDirectory,"amateur_delim.txt").toString());
					reader = new BufferedReader(new FileReader(veAmateurDelimitedTxtFile));

					// skip the first header record
					reader.readLine();
					// the header looks like this:
					// callsign;first_name;surname;address_line;city;prov_cd;postal_code;qual_a;qual_b;qual_c;qual_d;qual_e;club_name;club_name_2;club_address;club_city;club_prov_cd;club_postal_code

					while ((record = reader.readLine()) != null)
					{
						record = record.toUpperCase();
						fields = record.split(";", -1);
						callSign = fields[0].trim();
						firstName = ScrubFirstName(fields[1]);;
						//providence, qualA, qualB, qualC, qualD, qualE, clubName1, clubName2, clubProvidence;
						providence = fields[5].trim();
						qualA = fields[7].trim();
						qualB = fields[8].trim();
						qualC = fields[9].trim();
						qualD = fields[10].trim();
						qualE = fields[11].trim();
						clubName1 = fields[12].trim();
						clubName2 = fields[13].trim();
						clubProvidence = fields[16].trim();

						veTotalRawRecordsProcessed++; // count the raw records processed

						// seems to be the case on club calls, so we still want to add them
						if (providence.isEmpty() && !clubProvidence.isEmpty())
						{
							veClubCalls++;
							providence = clubProvidence;
						}

						// at this point if providence is empty, they will be excluded. callsign should never be empty at this point
						if (!callSign.isEmpty() && !providence.isEmpty())
						{
							// exclude them if they only have the basic qualification set to 'A', if any other options are set, include them
							if (qualA.equals("A") && qualB.isEmpty() && qualC.isEmpty() && qualD.isEmpty() && qualD.isEmpty())
							{
								veBasicLicensesExcluded++;
								addRecord = false;
							}
							else
							{
								addRecord = true;
							}
						}

						if (addRecord)
						{
							// build up an array of the strings to export / write to the file so we can get
							// stats to put in comments at the top of the call history file
							veTotalRecordsIncluded++;

							callHistoryRecord = callSign + ',' + firstName + ',' + providence + '\r' + '\n';
							bigListOfVECallsigns.add(callHistoryRecord);
						}
					} // while ripping through amateur_delim.txt ve callsign data records

				}	// if include ve callsign database

				// write out the header and other comments and then write out the callsign data
				AddComment(writer, headerFooter);
				AddComment(writer,"This file is intended to be used with N1MM as call history to resolve");
				AddComment(writer,"names and states during POTA activations.");
				AddComment(writer,"This call history file would be compatible with any N1MM contest");
				AddComment(writer,"that uses Name and Exch1 in the callsign entry window.");
				AddComment(writer, headerFooter);
				AddComment(writer,"Code to export FCC and VE callsign data written by Max NG7M (ng7m@arrl.net).");
				AddComment(writer,"Kudos to Ben (KI7KY) and Jon (K7CO) for data validation and performance testing.");
				AddComment(writer,headerFooter);
				AddComment(writer,"File created: " + GetDateTimeUTC());
				AddComment(writer,"File name: " + n1mmCallHistoryFileName);
				AddComment(writer,headerFooter);
				AddComment(writer,"Be patient when loading this large call history into N1MM when using a slow PC.");
				AddComment(writer,headerFooter);
				AddComment(writer,headerFooter);
				AddComment(writer,"US FCC call database statistics:");
				AddComment(writer,headerFooter);
				AddComment(writer, "Included: " + generalClassCount + " general class calls");
				AddComment(writer, "Included: " + advancedClassCount + " advanced class calls");
				AddComment(writer, "Included: " + extraClassCount + " extra class calls");
				AddComment(writer, "Included: " + clubCalls + " club calls");
				AddComment(writer, "Included: " + (generalClassCount + advancedClassCount + extraClassCount) +" general, advanced and extra class calls");
				AddComment(writer, "Included: " + (generalClassCount + advancedClassCount + extraClassCount + clubCalls) + " club, general, advanced and extra class calls");
				AddComment(writer,headerFooter);
				AddComment(writer,"Excluded call statistics:");
				AddComment(writer,headerFooter);
				AddComment(writer, "Excluded: " + allOtherLicenceClasses + " other license classes / technician and novice");
				AddComment(writer, "Excluded: " + activeButExpired + " active but expired or soon to expire");
				AddComment(writer, "Excluded: " + notActiveCount + " not active calls");
				AddComment(writer, "Excluded a total of: " + (notActiveCount + allOtherLicenceClasses) + " records");
				AddComment(writer,headerFooter);
				AddComment(writer,"Total raw callsign records processed in USA FCC callsign database: " + totalRecords);
				AddComment(writer, "Total USA FCC included callsign entries: " + (generalClassCount + advancedClassCount + extraClassCount + clubCalls));
				AddComment(writer,headerFooter);

				// add ve related comments and stats
				if (argumentValues.includeVECallHistory)
				{
					AddComment(writer,headerFooter);
					AddComment(writer,"VE Canadian call database statistics:");
					AddComment(writer,headerFooter);
					AddComment(writer,"Included: " + (veTotalRecordsIncluded - veClubCalls) + " individual licensee calls with privileges below 30 mHz");
					AddComment(writer,"Included: " + veClubCalls + " calls that appear to clubs");
					AddComment(writer,"   Total: " + veTotalRecordsIncluded + " individual licensee and club calls");
					AddComment(writer,headerFooter);
					AddComment(writer,"Excluded call statistics:");
					AddComment(writer,headerFooter);
					AddComment(writer,"Excluded: " + veBasicLicensesExcluded + " calls with Basic qualification with only privileges above 30 mHz");
					AddComment(writer,headerFooter);
					AddComment(writer,"Total raw callsign records processed in Canadian VE callsign database: " + veTotalRawRecordsProcessed);
					AddComment(writer, "Total Canadian VE included callsign entries: " + veTotalRecordsIncluded);
					AddComment(writer,headerFooter);
				}

				// write the N1MM call history header descriptor
				AddComment(writer,headerFooter);
				AddComment(writer,"The next entry defines the N1MM field descriptors for the included call data:");
				AddComment(writer,headerFooter);
				writer.write("!!Order!!,Call,Name,Exch1\r\n"); // N1MM History File specific header to describe what each field is
				AddComment(writer,headerFooter);
				AddComment(writer,(generalClassCount + advancedClassCount + extraClassCount + clubCalls) + " USA FCC callsign entries begin below:");
				AddComment(writer,headerFooter);

				// write out all the entries int the big list of callsigns to include in call history file
				for ( String listEntry : bigListOfCallsigns)
				{
					writer.write(listEntry);
				}
				AddComment(writer,headerFooter);
				AddComment(writer, "End of USA FCC callsigns. Number of USA FCC callsigns included above: " + (generalClassCount + advancedClassCount + extraClassCount + clubCalls));
				AddComment(writer,headerFooter);

				if (argumentValues.includeVECallHistory)
				{
					AddComment(writer,headerFooter);
					AddComment(writer,veTotalRecordsIncluded + " Canadian VE callsign entries begin below:");
					AddComment(writer,headerFooter);

					// write out all the entries int the big VE list of callsigns to include in call history file
					for (String listEntry : bigListOfVECallsigns)
					{
						writer.write(listEntry);
					}
					AddComment(writer, headerFooter);
					AddComment(writer, "End of Canadian VE callsigns. Number of Canadian VE callsigns included above: " + veTotalRecordsIncluded);
					AddComment(writer, headerFooter);
				}
			}
		} catch(FileNotFoundException e)
		{
			e.printStackTrace();
		} catch(IOException e)
		{
			e.printStackTrace();
		} catch (ParseException e)
        {
            throw new RuntimeException(e);
        } finally
		{
			if(reader != null)
			{
				try
				{
					reader.close();
				} catch(IOException e)
				{
					e.printStackTrace();
				}
			}
			if(writer != null)
			{
				try
				{
					writer.close();
				} catch(IOException e)
				{
					e.printStackTrace();
				}
			}
		}

		out(headerFooter);
		out("");
		return true;
	}

	private String ScrubFirstName(String firstName)
	{
		String[] fields;

		// trim, uppercase and remove any period or comma, FFC data won't have a comma, but FE firstnames could
		//firstName.trim().toUpperCase();
		firstName = firstName.trim().replaceAll("[.,]","").toUpperCase();

		// set first name to the longest first name grabbed from attentionLine
		fields = firstName.split(" ", -1);

		// if there are more than 3 parts just take the first two
		if (fields.length > 2)
		{
			firstName = fields[0] + " " + fields[1];
			fields = firstName.split(" ", -1);	// yeah lazy, removing the last element looks clunky from the code I looked up
		}

		// check for a single letter initial
		if (fields.length == 2) // if two names grab the longest if one is only one letter
		{
			if (fields[0].length() == 1 || fields[1].length() == 1)
			{
				firstName = (fields[0].length() > fields[1].length()) ? fields[0] : fields[1];
			}
			else
			{
				// just take the first name if there are two names at this point
				firstName = fields[0];
			}
		}

		return firstName;
	}

	private void AddComment(BufferedWriter writer, String comment) throws IOException
    {
		if (null != writer)
		{
			writer.write("# " + comment + "\r\n");
			if (getConfiguration().verboseOutput)
			{
				out(comment);
			}
		}
	}

	private String GetDateTimeUTC()
	{
		Instant instant = Instant.now();

		// Define the desired format, see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
		DateTimeFormatter formatter =
				DateTimeFormatter.ofPattern("E, M-dd-yyyy, HH:mm:ss z").withZone(ZoneId.of("UTC"));

		return formatter.format(instant);
	}

	private String GetN1MMPotaCallHisotryFileName()
	{
		Instant instant = Instant.now();

		// Define the desired format, see: https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
		DateTimeFormatter formatter =
				DateTimeFormatter.ofPattern("MM-dd-yyyy-z").withZone(ZoneId.of("UTC"));

		String formatted =  formatter.format(instant);
		return "POTA-" + formatted + ".txt";
	}


	private boolean validateFields(String fileName, int recordNum, StringBuilder sbRecord)
	{
		boolean fieldsOK = false;
		String splitBy = Pattern.quote("|");
		String record = sbRecord.toString();
		StringBuilder cleanedRecord = new StringBuilder();
		boolean verboseOutput = getConfiguration().verboseOutput;

		// check for non ascii chars
		if(asciiPattern.matcher(record).find())
		{
			if(verboseOutput)
			{
				out("NON ASCII characters found in record#: " + recordNum + " Full Record: " + record);
			}

			record = record.replaceAll("[^\\p{ASCII}]", "");

			if(verboseOutput)
			{
				out("Record#: " + recordNum + " After Cleaning: " + record);
			}
		}

		String[] fields = record.split(splitBy, -1);

		int fieldsPerRecord = 0;
		int[] fieldLengths = null;

		fileName = fileName.toUpperCase();
		switch(fileName)
		{
			case "AM.DAT":
				fieldsPerRecord = 18;
				fieldLengths = new int[] {2, 9, 14, 30, 10, 1, 1, 2, 10, 1, 1, 1, 1, 1, 12, 10, 1, 50};
				break;
			case "CO.DAT":
				fieldsPerRecord = 8;
				fieldLengths = new int[] {2, 9, 14, 10, 10, 255, 1, 10};
				break;
			case "EN.DAT":
				fieldsPerRecord = 27;
				fieldLengths = new int[] {2, 9, 14, 30, 10, 2, 9, 200, 20, 1, 20, 3, 10, 10, 50, 60, 20, 2, 9, 20, 35, 3, 10, 1, 40, 1, 10};
				break;
			case "HD.DAT":
				fieldsPerRecord = 51;
				fieldLengths = new int[] {2, 9, 14, 30, 10, 1, 2, 10, 10, 10, 10, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 20, 1, 20, 3, 40, 1, 1, 1, 1, 1, 1, 1, 10, 10, 2, 1, 1, 1, 1, 1, 1};
				break;
			case "HS.DAT":
				fieldsPerRecord = 6;
				fieldLengths = new int[] {2, 9, 14, 10, 10, 6};
				break;
			case "LA.DAT":
				fieldsPerRecord = 8;
				fieldLengths = new int[] {2, 9, 10, 1, 60, 10, 60, 1};
				break;
			case "SC.DAT":
				fieldsPerRecord = 9;
				fieldLengths = new int[] {2, 9, 14, 30, 10, 1, 4, 1, 10};
				break;
			case "SF.DAT":
				fieldsPerRecord = 11;
				fieldLengths = new int[] {2, 9, 14, 30, 10, 1, 9, 4, 255, 1, 10};
				break;
			default:
				out("Unknown file name passed to validateFieldLengths: " + fileName);
				break;
		}

		if(0 != fieldsPerRecord)
		{
			// check field count
			fieldsOK = (fields.length >= fieldsPerRecord);

			if(!fieldsOK)
			{
				if(verboseOutput)
				{
					out("Field Count Mismatch Found in Record: " + recordNum + " Expected Field Count: " + fieldsPerRecord + " Actual Field Count: " + fields.length);
				}
				fieldsOK = false;
			}

			if(fieldsOK)    // check each field length
			{
				for(int x = 0; x < fieldsPerRecord; x++)
				{
					// trim white space off each field, we find that some fields apparently have fat fingered leading and trailing white space
					fields[x] = fields[x].trim();

					if(fields[x].length() > fieldLengths[x])
					{
						if(verboseOutput)
						{
							out("Invalid field length found in Field#: " + x + " Value = " + fields[x] + " Max Length: " + fieldLengths[x] + " Actual Length: " + fields[x].length());
						}

						// trim to expected size
						fields[x] = fields[x].substring(0,fieldLengths[x]);

						if(verboseOutput)
						{
							out("Field length trimmed to documented length of: " + fieldLengths[x] + " New field value: " + fields[x]);
						}
					}
					cleanedRecord.append(fields[x]).append( (x < fieldsPerRecord -1) ? "|" : "");
				}
			}
		}

		if(fieldsOK)
		{
			record = cleanedRecord.toString();
			sbRecord.replace(0, record.length(), record);
		}

		return fieldsOK;
	}

	private void unZipFile(String outputFolder, String zipFile)
	{
		final int BUFFER = 262144;
		byte[] buffer = new byte[BUFFER];
		boolean error = false;

		try{

			File folder = new File(outputFolder);
			if(!folder.exists())
			{
				folder.mkdir();
			}

			Path zipFilePath = Paths.get(outputFolder, zipFile);
			ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath.toString()));

			ZipEntry ze = zis.getNextEntry();

			while(ze!=null)
			{

				String fileName = ze.getName();
				File newFile = new File(outputFolder + File.separator + fileName);

				out("Unzipping File: "+ newFile.getAbsoluteFile());

				new File(newFile.getParent()).mkdirs();

				FileOutputStream fos = new FileOutputStream(newFile);

				int len;
				while ((len = zis.read(buffer)) > 0)
				{
					fos.write(buffer, 0, len);
				}

				fos.close();
				ze = zis.getNextEntry();
			}

			zis.closeEntry();
			zis.close();

			out("Done Unzipping: " + zipFile);

		}
		catch(IOException e)
		{
			out("Error Unzipping file: " + e.getCause());
		}
	}

	private void createZipFile(String inputDirectory, String zipFileName)
	{
		try
		{
			final int BUFFER = 262144;
			BufferedInputStream origin;

			byte data[] = new byte[BUFFER];

			// get a list of files from current directory
			File f = new File(inputDirectory);
			String files[] = f.list();

			FilenameFilter filter = new FilenameFilter()
			{
				@Override
				public boolean accept(File dir, String name)
				{
					return name.toLowerCase().endsWith("");
				}
			};

			// delete existing zip file if it exists
			Files.deleteIfExists(new File(zipFileName).toPath());

			File inputDirFile = new File(inputDirectory);
			File[] filesInDir = inputDirFile.listFiles(filter);
			String absoluteFilePath;

			// don't create the output zip file until you have the output dir file list
			out("Creating ZipFile: " + zipFileName);
			FileOutputStream dest = new FileOutputStream(zipFileName);
			ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));

			for(File file : filesInDir)
			{
				absoluteFilePath = file.getAbsolutePath();
				out("Adding: " + file);
				FileInputStream fi = new FileInputStream(absoluteFilePath);
				origin = new BufferedInputStream(fi, BUFFER);
				ZipEntry entry = new ZipEntry(file.getName());
				out.putNextEntry(entry);
				int count;

				while((count = origin.read(data, 0, BUFFER)) != -1)
				{
					out.write(data, 0, count);
				}
				origin.close();
			}

			out.close();
			out("Done Creating ZipFile: " + zipFileName);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}


	private File outputDirectoryIsValid(String outputDestinationPath)
	{
		boolean bReturn = false;
		File file = null;

		if(outputDestinationPath == null)
		{
			out("Missing Output Destination Path.");
		}
		else
		{
			file = new File(outputDestinationPath);

			if(file.isDirectory() && file.canWrite())
			{
				out("Output path valid: " + file.getAbsolutePath());
				bReturn = true;
			}
			else
			{
				file = null;
				out("Invalid output directory!");
			}
		}

		return file;
	}

	private File workingDirectoryIsValid(String workingDestinationPath)
	{
		boolean bReturn = false;
		File file = null;

		if(workingDestinationPath == null)
		{
			out("Missing Working Destination Path.");
		}
		else
		{
			file = new File(workingDestinationPath);

			if(file.isDirectory() && file.canWrite())
			{
				out("Working path valid: " + file.getAbsolutePath());
				bReturn = true;
			}
			else
			{
				file = null;
				out("Invalid working directory!");
			}
		}

		return file;
	}

	private File n1mmCallHistoryOutputDirectoryIsValid(String directoryToValidate)
	{
		boolean bReturn = false;
		File file = null;

		if(directoryToValidate == null)
		{
			out("Missing N1MM call history output directory.");
		}
		else
		{
			file = new File(directoryToValidate);

			if(file.isDirectory() && file.canWrite())
			{
				out("N1MM Call history output path valid: " + file.getAbsolutePath());
				bReturn = true;
			}
			else
			{
				file = null;
				out("Invalid M1MM call history directory!");
			}
		}

		return file;
	}


	public void setConfiguration(CmdLineArgs cmdLineArgs)
	{
		_cmdLineArgs = cmdLineArgs;
	}

	public CmdLineArgs getConfiguration()
	{
		if(null == _cmdLineArgs)
		{
			throw new NullPointerException(Thread.currentThread().getStackTrace()[1].getClassName() + "::" + Thread.currentThread().getStackTrace()[1].getMethodName() + " Configuration not set!");
		}
		return _cmdLineArgs;
	}

	private void out(String str)
	{
		System.out.println(str);
	}

	boolean CopyFTPURLtoFile(String uri, String file)
	{
		boolean bSuccess = false;
		String exceptionCause = "";

		try
		{
			Downloader downloader = new Downloader();

			bSuccess = downloader.FTPDownload(new URI(uri), file);
		}
		catch (UnsupportedOperationException | URISyntaxException e)
		{
			exceptionCause = e.getCause().toString();
		}
        if (!bSuccess)
		{
			out("URL: " + uri + " to File: " + file + " failed!" + exceptionCause);
		}
		return bSuccess;
	}

	boolean CopyHTTPURLtoFile(String uri, String file)
	{
		boolean bSuccess = false;
		String exceptionCause = "";
		File httpFile = new File(file);

		try
		{
			Downloader downloader = new Downloader();

			File downloadedFile = downloader.httpDownload(new URI(uri), httpFile);
			bSuccess = downloadedFile.toString().equals(file);

		}
		catch (UnsupportedOperationException | URISyntaxException e)
		{
			exceptionCause = e.getCause().toString();
		}
		if (!bSuccess)
		{
			out("URL: " + uri + " to File: " + file + " failed!" + exceptionCause);
		}
		return bSuccess;
	}


}
