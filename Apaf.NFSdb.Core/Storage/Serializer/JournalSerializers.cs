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
using System.Collections.Concurrent;
using System.Threading;
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Storage.Serializer
{
    public class JournalSerializers
    {
        private static readonly Lazy<JournalSerializers> INSTANCE = new Lazy<JournalSerializers>(CreateSigleton, 
            LazyThreadSafetyMode.PublicationOnly);

        private readonly ConcurrentDictionary<string, Func<ISerializerFactory>> _factories =
            new ConcurrentDictionary<string, Func<ISerializerFactory>>();

        private static JournalSerializers CreateSigleton()
        {
            var instance = new JournalSerializers();
            instance.AddFactory(MetadataConstants.THRIFT_SERIALIZER_NAME, () => new ThriftSerializerFactory());
            instance.AddFactory(MetadataConstants.POCO_SERIALIZER_NAME, () => new PocoSerializerFactory());
            return instance;
        }

        public static JournalSerializers Instance
        {
            get { return INSTANCE.Value; }
        }

        public void AddFactory(string name, Func<ISerializerFactory> factory)
        {
            _factories[name] = factory;
        }

        public ISerializerFactory GetSerializer(string name)
        {
            return _factories[name].Invoke();
        }
    }
}