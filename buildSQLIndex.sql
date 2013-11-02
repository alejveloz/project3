CREATE INDEX IdxOnItemId ON Item(id);

CREATE INDEX IdxOnUserId ON User(id);

CREATE INDEX IdxOnCategory ON Category(name);

CREATE INDEX IdxOnBid ON Bid(iid);

CREATE INDEX IdxOnItemSeller ON ItemSeller(iid);

CREATE INDEX IdxOnItemCategory ON ItemCategory(iid);