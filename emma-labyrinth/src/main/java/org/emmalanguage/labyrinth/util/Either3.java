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

package org.emmalanguage.labyrinth.util;

public abstract class Either3<L, M, R> {

	/**
	 * Create a Left value of Either3
	 */
	public static <L, M, R> Either3<L, M, R> Left3(L value) {
		return new Left3<L, M, R>(value);
	}

	/**
	 * Create a Middle value of Either3
	 */
	public static <L, M, R> Either3<L, M, R> Middle3(M value) {
		return new Middle3<L, M, R>(value);
	}

	/**
	 * Create a Right value of Either3
	 */
	public static <L, M, R> Either3<L, M, R> Right3(R value) { return new Right3<L, M, R>(value); }

	/**
	 * Retrieve the Left value of Either3.
	 *
	 * @return the Left value
	 * @throws IllegalStateException
	 *             if not called on Left
	 */
	public abstract L left() throws IllegalStateException;

	/**
	 * Retrieve the Middle value of Either3.
	 *
	 * @return the Middle value
	 * @throws IllegalStateException
	 *             if not called on middle
	 */
	public abstract M middle() throws IllegalStateException;

	/**
	 * Retrieve the Right value of Either3.
	 *
	 * @return the Right value
	 * @throws IllegalStateException
	 *             if not called on Right
	 */
	public abstract R right() throws IllegalStateException;

	/**
	 *
	 * @return true if this is a Left value, false else
	 */
	public final boolean isLeft() {
		return getClass() == Left3.class;
	}

	/**
	 *
	 * @return true if this is a Middle value, false else
	 */
	public final boolean isMiddle() {
		return getClass() == Middle3.class;
	}

	/**
	 *
	 * @return true if this is a Right value, false else
	 */
	public final boolean isRight() {
		return getClass() == Right3.class;
	}

	/**
	 * A left value of {@link Either3}
	 *
	 * @param <L>
	 *            the type of Left
	 * @param <L>
	 *            the type in the Middle
	 * @param <R>
	 *            the type of Right
	 */
	public static class Left3<L, M, R> extends Either3<L, M, R> {
		private L value;

		public Left3(L value) {
			this.value = java.util.Objects.requireNonNull(value);
		}

		@Override
		public L left() {
			return value;
		}

		@Override
		public M middle() {
			throw new IllegalStateException("Cannot retrieve Right value in the Middle");
		}

		@Override
		public R right() {
			throw new IllegalStateException("Cannot retrieve Right value on a Left");
		}

		/**
		 * Sets the encapsulated value to another value
		 *
		 * @param value the new value of the encapsulated value
		 */
		public void setValue(L value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object object) {
			if (object instanceof Left3<?, ?, ?>) {
				final Left3<?, ?, ?> other = (Left3<?, ?, ?>) object;
				return value.equals(other.value);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}

		@Override
		public String toString() {
			return "Left(" + value.toString() + ")";
		}

		/**
		 * Creates a left value of {@link Either3}
		 *
		 */
		public static <L, M, R> Left3<L, M, R> of(L left) {
			return new Left3<L, M, R>(left);
		}
	}

	/**
	 * A middle value of {@link Either3}
	 *
	 * @param <L>
	 *            the type of Left
	 * @param <L>
	 *            the type in the Middle
	 * @param <R>
	 *            the type of Right
	 */
	public static class Middle3<L, M, R> extends Either3<L, M, R> {
		private M value;

		public Middle3(M value) { this.value = java.util.Objects.requireNonNull(value); }

		@Override
		public L left() { throw new IllegalStateException("Cannot retrieve Left value on a Middle"); }

		@Override
		public M middle() { return value; }

		@Override
		public R right() {
			throw new IllegalStateException("Cannot retrieve Right value on a Middle");
		}

		/**
		 * Sets the encapsulated value to another value
		 *
		 * @param value the new value of the encapsulated value
		 */
		public void setValue(M value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object object) {
			if (object instanceof Middle3<?, ?, ?>) {
				final Middle3<?, ?, ?> other = (Middle3<?, ?, ?>) object;
				return value.equals(other.value);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}

		@Override
		public String toString() {
			return "Middle(" + value.toString() + ")";
		}

		/**
		 * Creates a middle value of {@link Either3}
		 *
		 */
		public static <L, M, R> Middle3<L, M, R> of(M middle) {
			return new Middle3<L, M, R>(middle);
		}
	}


	/**
	 * A right value of {@link Either3}
	 *
	 * @param <L>
	 *            the type of Left
	 * @param <L>
	 *            the type in the Middle
	 * @param <R>
	 *            the type of Right
	 */
	public static class Right3<L, M, R> extends Either3<L, M, R> {
		private R value;

		private Left3<L, M, R> left;

		public Right3(R value) {
			this.value = java.util.Objects.requireNonNull(value);
		}

		@Override
		public L left() {
			throw new IllegalStateException("Cannot retrieve Left value on a Right");
		}

		@Override
		public M middle() {
			throw new IllegalStateException("Cannot retrieve Left value in the Middle");
		}

		@Override
		public R right() {
			return value;
		}

		/**
		 * Sets the encapsulated value to another value
		 *
		 * @param value the new value of the encapsulated value
		 */
		public void setValue(R value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object object) {
			if (object instanceof Right3<?, ?, ?>) {
				final Right3<?, ?, ?> other = (Right3<?, ?, ?>) object;
				return value.equals(other.value);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return value.hashCode();
		}

		@Override
		public String toString() {
			return "Right(" + value.toString() + ")";
		}

		/**
		 * Creates a right value of {@link Either3}
		 *
		 */
		public static <L, M, R> Right3<L, M, R> of(R right) { return new Right3<L, M, R>(right);

		}
	}
}
