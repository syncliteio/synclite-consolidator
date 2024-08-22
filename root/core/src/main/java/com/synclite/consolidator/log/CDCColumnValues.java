/*
 * Copyright (c) 2024 mahendra.chavan@synclite.io, all rights reserved.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.synclite.consolidator.log;

public class CDCColumnValues {
    public long columnIndex;
    public long beforeValue;
    public long afterValue;

    public CDCColumnValues(long columnIndex, long beforeValue, long afterValue) {
        this.columnIndex= columnIndex;
        this.beforeValue= beforeValue;
        this.afterValue = afterValue;
    }
}
