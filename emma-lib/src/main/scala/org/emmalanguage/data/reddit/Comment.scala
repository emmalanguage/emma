/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package data.reddit

case class Comment
( //@formatter:off
  author                 : String,
  author_flair_css_class : String,
  author_flair_text      : String,
  body                   : String,
  controversiality       : Option[Long],
  created_utc            : Option[Long],
  distinguished          : String,
  edited                 : Option[Boolean],
  gilded                 : Option[Long],
  id                     : String,
  link_id                : String,
  parent_id              : String,
  retrieved_on           : Option[Long],
  score                  : Option[Long],
  stickied               : Option[Boolean],
  subreddit              : String,
  subreddit_id           : String,
  ups                    : Option[Long]
) //@formatter:on
