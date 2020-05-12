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
		// private static string CurrentIndexName{ get; set; }

		static void Main(string[] args)
		{
			Client = NuSearchConfiguration.GetClient();
			var directory = args.Length > 0 && !string.IsNullOrEmpty(args[0]) 
				? args[0] 
				: NuSearchConfiguration.PackagePath;
			DumpReader = new NugetDumpReader(directory);
            // CurrentIndexName = NuSearchConfiguration.CreateIndexName();

			// No need to delete when doing SwapAlias...
            DeleteIndexIfExists();
            CreateIndex();
            IndexDumps();
            // SwapAlias();

			Console.WriteLine("Press ENTER to exit.");
			Console.Read();
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

        /* Index with DumpReader.GetPackges() */

        static void IndexDumps()
        {
            Console.WriteLine("Setting up a lazy xml files reader that yields packages...");
            // var packages = DumpReader.GetPackages().Take(1000);
            var packages = DumpReader.GetPackages();

            Console.Write("Indexing documents into Elasticsearch...");
            var waitHandle = new CountdownEvent(1);

            var bulkAll = Client.BulkAll(packages, b => b
                //.Index(CurrentIndexName)
                .BackOffRetries(2)
                .BackOffTime("30s")
                .RefreshOnCompleted(true)
                .MaxDegreeOfParallelism(4)
                .Size(1000)
            );

            ExceptionDispatchInfo captureInfo = null;

            bulkAll.Subscribe(new BulkAllObserver(
                onNext: b => Console.Write("."),
                onError: e =>
                {
                    captureInfo = ExceptionDispatchInfo.Capture(e);
                    waitHandle.Signal();
                },
                onCompleted: () => waitHandle.Signal()
            ));

            waitHandle.Wait();
            captureInfo?.Throw();
            Console.WriteLine("Done.");
        }

        /* Index with indexMany() shortcut API */
//         static void IndexDumps()
//         {
// 			Console.WriteLine("Reading packages...");
//             // packages from first dump file only
//             // var packages = DumpReader.Dumps.First().NugetPackages;
// 
//             // packages from all dumped files
// 			/*
//             var packages = DumpReader.Dumps.Select(i => i.NugetPackages).Aggregate(
//                  (a, b) => a.Concat(b).ToList());
//             */
//             var packages = DumpReader.Dumps.Take(50).Select(i => i.NugetPackages).Aggregate(
//                  (a, b) => a.Concat(b).ToList());
//             Console.WriteLine("Indexing documents into Elasticsearch...");
// 
//             // Use the shorthand IndexMany()
//             var result = Client.IndexMany(packages);
// 
//             if (!result.IsValid)
//             {
//                 foreach (var item in result.ItemsWithErrors)
//                     Console.WriteLine("Failed to index document {0}: {1}", item.Id, item.Error);
// 
//                 Console.WriteLine(result.DebugInformation);
//                 Console.Read();
//                 Environment.Exit(1);
//             }
// 
//             Console.WriteLine("Done.");
//         }
// 
        /* Index with indexMany API plus lambda
        static void IndexDumps()
        {
            var packages = DumpReader.Dumps.First().NugetPackages;

            Console.Write("Indexing documents into Elasticsearch...");

            // Use IndexMany() instead.
            var result = Client.Bulk(b => b.IndexMany(packages));

            if (!result.IsValid)
            {
                foreach (var item in result.ItemsWithErrors)
                    Console.WriteLine("Failed to index document {0}: {1}", item.Id, item.Error);

                Console.WriteLine(result.DebugInformation);
                Console.Read();
                Environment.Exit(1);
            }

            Console.WriteLine("Done.");
        }
		*/

        /* Index with Buik API
        static void IndexDumps()
        {
            var packages = DumpReader.Dumps.First().NugetPackages;

            Console.Write("Indexing documents into Elasticsearch...");

            // New bulk method.
            var result = Client.Bulk(b =>
            {
                foreach (var package in packages)
                {
                    b.Index<FeedPackage>(i => i.Document(package));
                }

                return b;
            });

            if (!result.IsValid)
            {
                foreach (var item in result.ItemsWithErrors)
                {
                    Console.WriteLine("Failed to index document {0}: {1}", item.Id, item.Error);
                }
            }

            Console.WriteLine("Done.");
        }
		*/

        /* Index single document at a time
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
		*/

        static void CreateIndex()
        {
            Client.Indices.Create(/* CurrentIndexName */ "nusearch", i => i
                .Settings(s => s
                    .NumberOfShards(2)
                    .NumberOfReplicas(0)
                    .Setting("index.mapping.nested_objects.limit", 12000)
                    .Analysis(analysis => analysis
                        .Tokenizers(tokenizers => tokenizers
                            .Pattern("nuget-id-tokenizer", p => p.Pattern(@"\W+"))
                        )
                        .TokenFilters(tokenfilters => tokenfilters
                            .WordDelimiter("nuget-id-words", w => w
                                .SplitOnCaseChange()
                                .PreserveOriginal()
                                .SplitOnNumerics()
                                .GenerateNumberParts(false)
                                .GenerateWordParts()
                            )
                        )
                        .Analyzers(analyzers => analyzers
                            .Custom("nuget-id-analyzer", c => c
                                .Tokenizer("nuget-id-tokenizer")
                                .Filters("nuget-id-words", "lowercase")
                            )
                            .Custom("nuget-id-keyword", c => c
                                .Tokenizer("keyword")
                                .Filters("lowercase")
                            )
                        )
                    )
                )
                .Map<Package>(map => map
                    .AutoMap()
                    .Properties(ps => ps
                        .Completion(c => c
                            .Name(p => p.Suggest)
                        )
                        .Nested<PackageVersion>(n => n
                            .Name(p => p.Versions.First())
                            .AutoMap()
                            .Properties(pps => pps
                                .Nested<PackageDependency>(nn => nn
                                    .Name(pv => pv.Dependencies.First())
                                    .AutoMap()
                                )
                            )
                        )
                        .Nested<PackageAuthor>(n => n
                            .Name(p => p.Authors.First())
                            .AutoMap()
                            .Properties(props => props
                                .Text(t => t
                                    .Name(a => a.Name)
                                    .Fielddata()
                                )
                            )
                        )
                    )
                )
            );
        }

		/*
        private static void SwapAlias()
        {
            var indexExists = Client.Indices.Exists(NuSearchConfiguration.LiveIndexAlias).Exists;

            Client.Indices.BulkAlias(aliases =>
            {
                if (indexExists)
                    aliases.Add(a => a
                        .Alias(NuSearchConfiguration.OldIndexAlias)
                        .Index(Client.GetIndicesPointingToAlias(NuSearchConfiguration.LiveIndexAlias).First())
                    );

                return aliases
                    .Remove(a => a.Alias(NuSearchConfiguration.LiveIndexAlias).Index("*"))
                    .Add(a => a.Alias(NuSearchConfiguration.LiveIndexAlias).Index(CurrentIndexName));
            });

            var oldIndices = Client.GetIndicesPointingToAlias(NuSearchConfiguration.OldIndexAlias)
                .OrderByDescending(name => name)
                .Skip(2);

            foreach (var oldIndex in oldIndices)
                Client.Indices.Delete(oldIndex);
        }
		*/
    }
}

