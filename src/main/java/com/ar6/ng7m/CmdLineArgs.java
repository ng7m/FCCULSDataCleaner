package com.ar6.ng7m;

import net.sourceforge.argparse4j.annotation.Arg;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CmdLineArgs
{
	@Arg(dest = "r")
    public boolean readOnly;

	@Arg(dest = "o")
    private String outputDestination="";
    public String GetOutputDestination()
    {
		// if output path is a blank string (the default), then use OS level temp path and
		// create a directory below the temp path

		if (outputDestination.isEmpty())
		{
			// Get the temporary directory and print it.
			String tempDir = System.getProperty("java.io.tmpdir");

			Path filePath = Paths.get(tempDir, "FCCULSDataCleaner/cleaned");
			SetOutputDestination(filePath.toString());
		}

		ValidateOrCreateDirectory(outputDestination);

    	return outputDestination;
    }

	private void ValidateOrCreateDirectory(String outputDestination)
	{
		boolean directoryExists = false;
		// check to see if directory exists and create it if not
		File file = new File(outputDestination);

		if(file.isDirectory() && file.canWrite())
		{
			directoryExists = true;
		}

		if (!directoryExists)
		{
			file.mkdirs();
			GetOutputDestination();
		}
	}

	public void SetOutputDestination(String _outputDestination)
	{
		outputDestination = _outputDestination;
	}

	@Arg(dest = "v")
	public boolean verboseOutput;
	public boolean isVerboseOutput()
	{
		return verboseOutput;
	}

	@Arg(dest = "d")
	public boolean downloadFccZip;

	@Arg(dest = "dve")
	public boolean downloadVEZip;

	@Arg(dest = "fccAmURL")
	public String fccAmatuerLicenseCompleteURL;

	@Arg(dest = "veAmURL")
	public String veAmatuerLicenseCompleteURL;

	@Arg(dest = "z")
	public boolean createOutputZipFile;

	@Arg(dest = "n1mmch")
	public boolean createN1MMCallHistory;

	@Arg(dest = "ivech")
	public boolean includeVECallHistory;

	@Arg(dest = "zf")
	private String zipFileName;
	String GetZipFileName()
	{
		return zipFileName;
	}

	@Arg(dest = "vezf")
	private String veZipFileName;
	String GetVeZipFileName()
	{
		return veZipFileName;
	}

	@Arg(dest = "wd")
	private String workingDirectory="";
	public String GetWorkingDirectory()
	{
		// default to a working direct / temp OS path
		if (workingDirectory.isEmpty())
		{
			// Get the temporary directory and print it.
			String tempDir = System.getProperty("java.io.tmpdir");

			Path filePath = Paths.get(tempDir, "FCCULSDataCleaner/temp");
			workingDirectory = filePath.toString();

			System.out.println("Using OS Temp File Path as working Directory: " + workingDirectory);
		}

		ValidateOrCreateDirectory(workingDirectory);

		return workingDirectory;
	}

	@Arg(dest = "n1mmchod")
	private String n1mmCallHistoryOutputDirectory="";
	public String GetN1MMCallHistoryOutputDirectory()
	{
		// default to a working direct / temp OS path
		if (n1mmCallHistoryOutputDirectory.isEmpty())
		{
			// Get the temporary directory and print it.
			String tempDir = System.getProperty("java.io.tmpdir");

			Path filePath = Paths.get(tempDir, "FCCULSDataCleaner/N1MMCallHistory");
			n1mmCallHistoryOutputDirectory = filePath.toString();

			System.out.println("Using OS Temp File Path as N1MM call history output directory: " + n1mmCallHistoryOutputDirectory);
		}

		ValidateOrCreateDirectory(n1mmCallHistoryOutputDirectory);

		return n1mmCallHistoryOutputDirectory;
	}

	@Arg(dest = "vewd")
	private String veN1MMCallHistoryWorkingDirectory="";
	public String GetVEN1MMCallHistoryWorkingDirectory()
	{
		// default to a working direct / temp OS path
		if (veN1MMCallHistoryWorkingDirectory.isEmpty())
		{
			// Get the temporary directory and print it.
			String tempDir = System.getProperty("java.io.tmpdir");

			Path filePath = Paths.get(tempDir, "VEAmateurDB/temp");
			veN1MMCallHistoryWorkingDirectory = filePath.toString();

			System.out.println("Using OS Temp File Path as VE Database working Directory: " + veN1MMCallHistoryWorkingDirectory);
		}

		ValidateOrCreateDirectory(veN1MMCallHistoryWorkingDirectory);

		return veN1MMCallHistoryWorkingDirectory;
	}



}
