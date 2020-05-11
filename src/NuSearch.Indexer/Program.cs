using System;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using Nest;
using NuSearch.Domain;
using NuSearch.Domain.Data;
using NuSearch.Domain.Model;

namespace NuSearch.Indexer
{
	class Program
	{
		private static ElasticClient Client { get; set; }
		private static NugetDumpReader DumpReader { get; set; }

		static void Main(string[] args)
		{
			Client = NuSearchConfiguration.GetClient();
			var directory = args.Length > 0 && !string.IsNullOrEmpty(args[0]) 
				? args[0] 
				: NuSearchConfiguration.PackagePath;
			DumpReader = new NugetDumpReader(directory);

			DeleteIndexIfExists();
			IndexDumps();

			Console.WriteLine("Press any key to exit.");
			Console.ReadKey();
		}

		// Add this method.
		static void DeleteIndexIfExists()
		{
			if (Client.Indices.Exists("nusearch").Exists) {
				Console.WriteLine("Found existing nusearch index, deleting...");
			    Client.Indices.Delete("nusearch");
				Console.WriteLine("Index nusearch deleted.");
            }
		}

        static void IndexDumps()
        {
            var packages = DumpReader.Dumps.First().NugetPackages;

            Console.Write("Indexing documents into Elasticsearch...");

            foreach (var package in packages)
            {
                var result = Client.IndexDocument(package);

                if (!result.IsValid)
                {
                    Console.WriteLine(result.DebugInformation);
                    Console.Read();
                    Environment.Exit(1);
                }
            }

            Console.WriteLine("Done.");
        }
    }
}

