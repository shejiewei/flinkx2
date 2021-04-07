/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.logminer.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import org.apache.commons.lang3.StringUtils;


public class LogminerOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private LogminerOutputFormat format;

    public LogminerOutputFormatBuilder() {
        super.format = format = new LogminerOutputFormat();
    }

    public void setUrl(String url) {
        format.url = url;
    }


    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setDelayTime(int delayTime) {
        format.delaytime = delayTime;
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

    }
}
