package com.ar6.ng7m;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
	private static Pattern asciiPattern = Pattern.compile("[^\\p{ASCII}]");


	public boolean Execute()
	{
		double startTime = System.currentTimeMillis();
		boolean bSuccessful = false;
		CmdLineArgs argumentValues = getConfiguration();
		String outputDestinationPath = argumentValues.GetOutputDestination();
		File outputDirectory = outputDirectoryIsValid(outputDestinationPath);

		if(null != outputDirectory)
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
				Path filePath = Paths.get(argumentValues.GetWorkingDirOrFiles()[0], argumentValues.GetZipFileName());
                // reach out and download the file, make sure and set proxy jvm properties if
                // running from a NAT based network
                out("Downloading: " + argumentValues.fccAmatuerLicenseCompleteURL);
                out("Be Patient! File may be 130+ megs in size...");

                if (CopyURLtoFile(argumentValues.fccAmatuerLicenseCompleteURL, filePath.toString()))
                {
                    // if we made it here, we have a zip file that needs to be extracted
                    unZipFile(argumentValues.GetWorkingDirOrFiles()[0], argumentValues.GetZipFileName());

                    bSuccessful = true;
                }
			}
			if(bSuccessful)
			{
				String[] workingDirOrFiles = argumentValues.GetWorkingDirOrFiles();
				out("Begin File Processing...");
				for(int x = 0; x < workingDirOrFiles.length; x++)
				{
					File datFile = new File(workingDirOrFiles[x]);

					if(datFile.isDirectory())
					{
						// copy everything to destination
						try
						{
							if(!argumentValues.readOnly)
							{
								out("Copying source directory to output directory...");
								FileUtils.copyDirectory(datFile, outputDirectory);
							}
						} catch(IOException e)
						{
							out("Error: " + e.getCause());
						}

						FilenameFilter filter = new FilenameFilter()
						{
							@Override
							public boolean accept(File dir, String name)
							{
								return name.toLowerCase().endsWith(".dat");
							}
						};

						File[] filesInDir = datFile.listFiles(filter);

						for(File file : filesInDir)
						{
							parse(file.getAbsolutePath(), file.getName(), outputDestinationPath);
						}

						if(argumentValues.createOutputZipFile)
						{
							Path path = Paths.get(outputDestinationPath, argumentValues.GetZipFileName());

							createZipFile(outputDestinationPath, path.toAbsolutePath().toString());
						}
					}
					else
					{
						out("Processing: " + workingDirOrFiles[x]);
						parse(datFile.getAbsolutePath(), datFile.getName(), outputDestinationPath);
					}
				}
				out("Finished Processing...");
				out("Processing Completed in: " + (System.currentTimeMillis() - startTime) / 1000 + " Seconds...");
			}
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

			while((record = reader.readLine()) != null)
			{
				++recordNum;
				StringBuilder sbRecord = new StringBuilder(record);

				if(!validateFields(fileName, recordNum, sbRecord))
				{
					++numErrors;
					out("Error found in record: " + recordNum + " with value: " + "\"" + record + "\"");
				}
				else
				{
					// assume record could have been cleaned up and reassign record to sbRecord
					record = sbRecord.toString();
					if(null != writer)
					{
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

	private boolean validateFields(String fileName, int recordNum, StringBuilder sbRecord)
	{
		boolean fieldsOK = false;
		String splitBy = Pattern.quote("|");
		String record = sbRecord.toString();
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

		// use comma as separator
		String[] fields = record.split(splitBy, -1);
		int fieldsPerRecord = 0;
		int fieldLengths[] = null;

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

						fieldsOK = true;
						break;
					}
				}
			}
		}

		if(fieldsOK)
		{
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
				out("Output Path Valid: " + file.getAbsolutePath());
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

	boolean CopyURLtoFile(String url, String file)
	{
		boolean bSuccess = false;
		String exceptionCause = "";

		try
		{
			Downloader downloader = new Downloader();
			downloader.download(new URL(url), new File(file));
			bSuccess = true;
		}
		catch (UnsupportedOperationException e)
		{
			exceptionCause = e.getCause().toString();
		}
		catch (IOException e)
		{
			exceptionCause = e.getCause().toString();
		}
		if (!bSuccess)
		{
			out("URL: " + url + " to File: " + file + " failed!" + exceptionCause);
		}
		return bSuccess;
	}

}
