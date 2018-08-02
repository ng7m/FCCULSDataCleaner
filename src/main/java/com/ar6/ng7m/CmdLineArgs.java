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
    private String outputDestination;
    public String GetOutputDestination()
    {
		// if output path is a blank string (the default), then use OS level temp path and
		// create a directory below the temp path

		if (0 == outputDestination.length())
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

	@Arg(dest = "fccAmURL")
	public String fccAmatuerLicenseCompleteURL;

	@Arg(dest = "z")
	public boolean createOutputZipFile;

	@Arg(dest = "zf")
	private String zipFileName;
	String GetZipFileName()
	{
		return zipFileName;
	}

	@Arg(dest = "wd")
	private String workingDirOrFiles[];

	public String[] GetWorkingDirOrFiles()
	{
		if (null != workingDirOrFiles)
		{
			// default to a working direct / temp OS path
			if (0 == workingDirOrFiles[0].length())
			{
				// Get the temporary directory and print it.
				String tempDir = System.getProperty("java.io.tmpdir");

				Path filePath = Paths.get(tempDir, "FCCULSDataCleaner/temp");
				workingDirOrFiles[0] = filePath.toString();

                System.out.println("Using OS Temp File Path as working Directory: " + workingDirOrFiles[0]);
			}
		}

		return workingDirOrFiles;
	}
	public void SetWorkingDirOrFiles(String[] _workingDirOrFiles)
	{
		workingDirOrFiles = _workingDirOrFiles;
	}

}
