package org.emmalanguage

import shapeless.labelled.FieldType

package object util {

  type =?>[-A, +R] = PartialFunction[A, R]
  type ->>[K, +V] = FieldType[K, V]
}
