package com.ar6.ng7m;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class FCCULSDataCleaner
{
	// external library objects
	private static ArgumentParserBuilder _argsParserBuilder = null;
	private static ArgumentParser _argsParser = null;

	// local project objects
	private static CmdLineArgs _cmdLineArgs = null;
	private static AmatCleaner _amatCleaner = null;
	
    public static void main(String[] args)
    {
		int	nExitValue;
    	try
    	{
	    	// parse the commandline args
	    	boolean argsParsed = parseArgs(args);

			if (argsParsed)
			{
				CmdLineArgs cmdLineArgs = getCmdLineArgs();

				// if execution makes it here we are happy with the input args
				AmatCleaner amatCleaner = getAmatCleaner();
				amatCleaner.setConfiguration(cmdLineArgs);

				// start the actual processing
				nExitValue = amatCleaner.Execute() ? 0 : 1;
			}
			else
			{
				nExitValue = 0;
			}
	    	

    	}
    	catch (Exception e)
    	{
			System.out.println(e.getMessage());
			System.out.println("-------------------------------------------------------");
			e.printStackTrace();
    		nExitValue = 1;
    	}
    	
    	System.exit(nExitValue);
    }	// end of main
    
    private static boolean parseArgs(String [] args)
    {
		boolean bSuccess = true;
    	ArgumentParser argsParser = getARGSParser();
		try
		{
			argsParser.parseArgs(args, getCmdLineArgs());
		}
		catch (ArgumentParserException e)
		{
			bSuccess = false;
		    argsParser.handleError(e);
		}
		return bSuccess;
    }
 
    private static CmdLineArgs getCmdLineArgs()
    {
    	if (_cmdLineArgs == null)
    	{
    		_cmdLineArgs = new CmdLineArgs();
    	}
    	return _cmdLineArgs;
    }
    
    private static ArgumentParser getARGSParser()
    {
    	// setup instantiate singleton argument parser
    	if (_argsParserBuilder == null)
    	{
    		String className = Thread.currentThread().getStackTrace()[1].getClassName();

    		_argsParserBuilder = ArgumentParsers.newFor(className);
    		_argsParser = _argsParserBuilder.build();

		     _argsParser.description("Validates / Cleans and re-creates FCC ULS license amateur data for import by AR-Cluster version 6 by AB5K.");

			_argsParser.addArgument("-r")
					.type(Boolean.class).setDefault(Boolean.FALSE)
					.help("Readonly Output.");

    		_argsParser.addArgument("-v")
    			.type(Boolean.class).setDefault(Boolean.FALSE)
        		.help("Verbose output.");

    		_argsParser.addArgument("-z")
    			.type(Boolean.class).setDefault(Boolean.TRUE)
        		.help("Create Zip File.");

    		_argsParser.addArgument("-zf")
    			.type(Boolean.class).setDefault("l_amat.zip")
        		.help("Overrides Zip File Name where l_amat.zip is the default.");

			_argsParser.addArgument("-d")
					.type(Boolean.class).setDefault(Boolean.TRUE)
					.help("Download FCC ULS Data using -fccAmURL URL Specified.");

			_argsParser.addArgument("-n1mmch")
					.type(Boolean.class).setDefault(Boolean.FALSE)
					.help("Create N1MM Call History File.");

			_argsParser.addArgument("-n1mmchod")
					.type(String.class).setDefault("")
					.help("Output Directory for N1MM call history file generation. Uses Temp Location if not specified.");

			_argsParser.addArgument("-fccAmURL")
					.type(String.class).setDefault("ftp://wirelessftp.fcc.gov/pub/uls/complete/l_amat.zip")
					.help("FCC URL to ULS Amateur Complete Zipped Data Download:");

			_argsParser.addArgument("-o")
					.type(String.class).setDefault("")
					.help("Output Directory for new zip file. Uses OS temp location if not specified.");

			_argsParser.addArgument("-wd")
					.type(String.class).setDefault("")
					.help("Working directory for -fccAmURL download. Uses OS temp location if not specified.");


		}
    	return _argsParser;
    }
    
    private static AmatCleaner getAmatCleaner()
    {
    	if (_amatCleaner == null)
    	{
    		_amatCleaner = new AmatCleaner();
    	}
    	return _amatCleaner;
    }
    
}
 