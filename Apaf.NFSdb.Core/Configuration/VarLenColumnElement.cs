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

using System.Runtime.Serialization;
using System.Xml.Serialization;

namespace Apaf.NFSdb.Core.Configuration
{
    public abstract class VarLenColumnElement : ColumnElement
    {
        protected VarLenColumnElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            IsNull = true;
            MaxSizeSerialization = -1;
            AvgSizeSerialization = -1;
        }

        [XmlAttribute("maxsize")]
        public int MaxSizeSerialization { get; set; }

        [XmlIgnore]
        public bool MaxSizeSerializationSpecified { get { return MaxSizeSerialization >= 0; } }

        [XmlIgnore]
        public int? MaxSize
        {
            get
            {
                if (MaxSizeSerialization >= 0) return MaxSizeSerialization;
                return null;
            }
            set { MaxSizeSerialization = value ?? -1; }
        }

        [XmlAttribute("avgsize")]
        public int AvgSizeSerialization { get; set; }

        [XmlIgnore]
        public bool AvgSizeSerializationSpecified { get { return AvgSizeSerialization >= 0; } }

        [XmlIgnore]
        public int? AvgSize
        {
            get
            {
                if (AvgSizeSerialization >= 0) return AvgSizeSerialization;
                return null;
            }
            set { AvgSizeSerialization = value ?? -1; }
        }
    }
}