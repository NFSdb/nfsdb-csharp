﻿#region copyright
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
using Apaf.NFSdb.Core.Column;

namespace Apaf.NFSdb.Core.Configuration
{
    [XmlRoot("string")]
    public class StringElement : ColumnElement
    {
        public StringElement()
        {
            OnDeserializing();
        }

        [OnDeserializing]
        private void OnDeserializing()
        {
            AvgSize = MetadataConstants.DEFAULT_STRING_AVG_SIZE;
            MaxSize = MetadataConstants.DEFAULT_STRING_MAX_SIZE;
        }

        public override EFieldType ColumnType
        {
            get { return EFieldType.String; }
        }
    }
}