#region ENBREA - Copyright (C) 2021 STÜBER SYSTEMS GmbH
/*    
 *    ENBREA
 *    
 *    Copyright (C) 2021 STÜBER SYSTEMS GmbH
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
#endregion

using Enbrea.BbsPlanung.Db;
using Enbrea.Csv;
using Enbrea.Ecf;
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ecf.BbsPlanung
{
    public class ExportManager : CustomManager
    {
        private int _recordCounter = 0;
        private int _tableCounter = 0;

        public ExportManager(
            Configuration config,
            CancellationToken cancellationToken = default,
            EventWaitHandle cancellationEvent = default)
            : base(config, cancellationToken, cancellationEvent)
        {
        }

        public async override Task Execute()
        {
            await using var bbsPlanungDbReader = new BbsPlanungDbReader(_config.EcfExport.DatabaseConnection);
            try
            {
                // Init counters
                _tableCounter = 0;
                _recordCounter = 0;

                // Report status
                Console.WriteLine("[Extracting] Start...");

                // Preperation
                PrepareExportFolder();

                // Connect reader
                await bbsPlanungDbReader.ConnectAsync();

                // Education
                await Execute(EcfTables.Teachers, bbsPlanungDbReader, async (r, w, h) => await ExportTeachers(r, w, h));
                await Execute(EcfTables.SchoolClasses, bbsPlanungDbReader, async (r, w, h) => await ExportSchoolClasses(r, w, h));
                await Execute(EcfTables.Students, bbsPlanungDbReader, async (r, w, h) => await ExportStudents(r, w, h));
                await Execute(EcfTables.StudentSchoolClassAttendances, bbsPlanungDbReader, async (r, w, h) => await ExportStudentSchoolClassAttendances(r, w, h));
                await Execute(EcfTables.StudentSubjects, bbsPlanungDbReader, async (r, w, h) => await ExportStudentSubjects(r, w, h));

                // Disconnect reader
                await bbsPlanungDbReader.DisconnectAsync();

                // Report status
                Console.WriteLine($"[Extracting] {_tableCounter} table(s) and {_recordCounter} record(s) extracted");
            }
            catch 
            {
                // Report error 
                Console.WriteLine();
                Console.WriteLine($"[Error] Extracting failed. Only {_tableCounter} table(s) and {_recordCounter} record(s) extracted");
                throw;
            }
        }

        private async Task Execute(string ecfTableName, BbsPlanungDbReader bbsPlanungDbReader, Func<BbsPlanungDbReader, EcfTableWriter, string[], Task<int>> action)
        {
            if (ShouldExportTable(ecfTableName, out var ecfFile))
            {
                // Report status
                Console.WriteLine($"[Extracting] [{ecfTableName}] Start...");

                // Generate ECF file name
                var ecfFileName = Path.ChangeExtension(Path.Combine(_config.EcfExport?.TargetFolderName, ecfTableName), "csv");

                // Create ECF file for export
                using var ecfWriterStream = new FileStream(ecfFileName, FileMode.Create, FileAccess.ReadWrite, FileShare.None);

                // Create ECF Writer
                using var ecfWriter = new CsvWriter(ecfWriterStream, Encoding.UTF8);

                // Call table specific action
                var ecfRecordCounter = await action(bbsPlanungDbReader, new EcfTableWriter(ecfWriter), ecfFile?.Headers);

                // Inc counters
                _recordCounter += ecfRecordCounter;
                _tableCounter++;

                // Report status
                Console.WriteLine($"[Extracting] [{ecfTableName}] {ecfRecordCounter} record(s) extracted");
            }
        }

        private async Task<int> ExportSchoolClasses(BbsPlanungDbReader bbsPlanungDbReader, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.Notes);
            }

            await foreach (var schoolClass in bbsPlanungDbReader.SchoolClassesAsync(_config.EcfExport.SchoolNo))
            {
                ecfTableWriter.TrySetValue(EcfHeaders.Id, schoolClass.Code);
                ecfTableWriter.TrySetValue(EcfHeaders.Code, schoolClass.Code);
                ecfTableWriter.TrySetValue(EcfHeaders.Teacher1Id, schoolClass.Teacher);
                ecfTableWriter.TrySetValue(EcfHeaders.Notes, schoolClass.Notes);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportStudents(BbsPlanungDbReader bbsPlanungDbReader, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.LastName,
                    EcfHeaders.FirstName,
                    EcfHeaders.Gender,
                    EcfHeaders.Birthdate,
                    EcfHeaders.StudentNo);
            }

            await foreach (var student in bbsPlanungDbReader.StudentsAsync(_config.EcfExport.SchoolNo))
            {
                ecfTableWriter.TrySetValue(EcfHeaders.Id, student.Id.ToString());
                ecfTableWriter.TrySetValue(EcfHeaders.LastName, student.Lastname);
                ecfTableWriter.TrySetValue(EcfHeaders.FirstName, student.Firstname);
                ecfTableWriter.TrySetValue(EcfHeaders.Gender, student.GetGender());
                ecfTableWriter.TrySetValue(EcfHeaders.Birthdate, student.GetBirthdate());
                ecfTableWriter.TrySetValue(EcfHeaders.StudentNo, student.StudentNo);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportStudentSchoolClassAttendances(BbsPlanungDbReader bbsPlanungDbReader, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.StudentId,
                    EcfHeaders.SchoolClassId);
            }

            await foreach (var student in bbsPlanungDbReader.StudentsAsync(_config.EcfExport.SchoolNo))
            {
                ecfTableWriter.TrySetValue(EcfHeaders.Id, student.Id.ToString() + "_" + student.SchoolClass);
                ecfTableWriter.TrySetValue(EcfHeaders.StudentId, student.Id.ToString());
                ecfTableWriter.TrySetValue(EcfHeaders.SchoolClassId, student.SchoolClass);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportStudentSubjects(BbsPlanungDbReader bbsPlanungDbReader, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.StudentId,
                    EcfHeaders.SchoolClassId);
            }

            await foreach (var student in bbsPlanungDbReader.StudentsAsync(_config.EcfExport.SchoolNo))
            {
                ecfTableWriter.TrySetValue(EcfHeaders.Id, student.Id.ToString() + "_" + student.SchoolClass);
                ecfTableWriter.TrySetValue(EcfHeaders.StudentId, student.Id.ToString());
                ecfTableWriter.TrySetValue(EcfHeaders.SchoolClassId, student.SchoolClass);

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }

        private async Task<int> ExportTeachers(BbsPlanungDbReader bbsPlanungDbReader, EcfTableWriter ecfTableWriter, string[] ecfHeaders)
        {
            var ecfRecordCounter = 0;

            if (ecfHeaders != null && ecfHeaders.Length > 0)
            {
                await ecfTableWriter.WriteHeadersAsync(ecfHeaders);
            }
            else
            {
                await ecfTableWriter.WriteHeadersAsync(
                    EcfHeaders.Id,
                    EcfHeaders.Code,
                    EcfHeaders.LastName,
                    EcfHeaders.FirstName,
                    EcfHeaders.Gender,
                    EcfHeaders.Birthdate);
            }

            await foreach (var teacher in bbsPlanungDbReader.TeachersAsync(_config.EcfExport.SchoolNo))
            {
                ecfTableWriter.TrySetValue(EcfHeaders.Id, teacher.Id);
                ecfTableWriter.TrySetValue(EcfHeaders.Code, teacher.Code);
                ecfTableWriter.TrySetValue(EcfHeaders.LastName, teacher.Lastname);
                ecfTableWriter.TrySetValue(EcfHeaders.FirstName, teacher.Firstname);
                ecfTableWriter.TrySetValue(EcfHeaders.Gender, teacher.GetGender());
                ecfTableWriter.TrySetValue(EcfHeaders.Birthdate, teacher.GetBirthdate());

                await ecfTableWriter.WriteAsync();

                ecfRecordCounter++;
            }

            return ecfRecordCounter;
        }
    }
}
