#region copyright
/*
 * Copyright (c) 2014. APAF http://apafltd.co.uk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System;
using System.IO;

namespace Apaf.NFSdb.Tests.Common
{
    public class DisposableTempDir : IDisposable
    {
        private const string WORKING_DIR = "partition_tests";
        private readonly string _dirName;

        public DisposableTempDir()
        {
            _dirName = WORKING_DIR + Guid.NewGuid();
        }

        public string DirName
        {
            get { return _dirName; }
        }

        public void Dispose()
        {
            if (Directory.Exists(_dirName))
            {
                Directory.Delete(_dirName, true);
            }
        }
    }
}