/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.logminer.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by shejiewei on 2020/12/11.
 */
public class LogminerInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected LogminerInputFormat format;
    public LogminerInputFormatBuilder() {
        super.format = this.format = new LogminerInputFormat();
    }

   public void setResetOffset(boolean resetOffset){
        format.resetOffset=resetOffset;
   }
   public void setFetchSize(int fetchSize){
       format.fetchSize=fetchSize;
   }

    public void setUsername(String username) {
        format.username = username;
    }
    public void setID(String id) {
        format.id = id;
    }
    public void setPassword(String password) {
        format.password = password;
    }

    public void setUrl(String url) {
        format.url = url;
    }

    public void setDatabaseName(String databaseName) {
        format.databaseName = databaseName;
    }

    public void setPavingData(boolean pavingData) {
        format.pavingData = pavingData;
    }

    public void setWhiteList(List<String> whiteList) {
       format.whiteList=whiteList;
    }
    public void setBlackList(List<String> blackList) {
       format.blackList=blackList;
    }
    public void setCat(String cat) {
        format.cat = cat;
    }

    public void setStatusInterval(Integer interval) {
        format.interval = interval;
    }

    public void setScn(Long scn) {
        format.scn = scn;
    }


    public void setTemporary(Boolean temporary) {
        format.temporary = temporary;
    }

    @Override
    protected void checkFormat() {
        if (StringUtils.isBlank(format.username)) {
            throw new IllegalArgumentException("No username supplied");
        }
        if (StringUtils.isBlank(format.password)) {
            throw new IllegalArgumentException("No password supplied");
        }
        if (StringUtils.isBlank(format.url)) {
            throw new IllegalArgumentException("No url supplied");
        }
        if (StringUtils.isBlank(format.databaseName)) {
            throw new IllegalArgumentException("No databaseName supplied");
        }
        if (CollectionUtils.isEmpty(format.whiteList)) {
            throw new IllegalArgumentException("No tableList supplied");
        }

    }
}
