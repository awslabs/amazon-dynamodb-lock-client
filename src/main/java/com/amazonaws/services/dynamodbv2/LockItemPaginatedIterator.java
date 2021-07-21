package com.amazonaws.services.dynamodbv2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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
