/**
 * Copyright 2013-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://aws.amazon.com/asl/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Paginated iterator for lock items.
 */
public abstract class LockItemPaginatedIterator implements Iterator<LockItem> {
  protected List<LockItem> currentPageResults = Collections.emptyList();
  protected int currentPageResultsIndex = 0;

  @Override
  public boolean hasNext() {
    while (this.currentPageResultsIndex == this.currentPageResults.size() && this.hasAnotherPageToLoad()) {
      this.loadNextPageIntoResults();
    }

    return this.currentPageResultsIndex < this.currentPageResults.size();
  }

  @Override
  public LockItem next() throws NoSuchElementException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }

    final LockItem next = this.currentPageResults.get(this.currentPageResultsIndex);
    this.currentPageResultsIndex++;

    return next;
  }

  @Override
  public void remove() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("This iterator is immutable.");
  }

  protected abstract boolean hasAnotherPageToLoad();

  protected abstract boolean hasLoadedFirstPage();

  protected abstract void loadNextPageIntoResults();
}
