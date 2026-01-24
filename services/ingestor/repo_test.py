"""Tests for ingestor repository with external_id functionality."""

import pytest
from unittest.mock import AsyncMock, patch


class TestGetHotelByExternalId:
    """Tests for get_hotel_by_external_id function."""

    @pytest.mark.asyncio
    async def test_returns_hotel_id_when_found(self):
        """Should return hotel ID when external_id exists."""
        from services.ingestor import repo

        mock_conn = AsyncMock()
        mock_result = {"id": 123}

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_external_id = AsyncMock(return_value=mock_result)

                result = await repo.get_hotel_by_external_id("texas_hot", "123:456")

                assert result == 123
                mock_queries.get_hotel_by_external_id.assert_called_once_with(
                    mock_conn, external_id_type="texas_hot", external_id="123:456"
                )

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self):
        """Should return None when external_id doesn't exist."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_external_id = AsyncMock(return_value=None)

                result = await repo.get_hotel_by_external_id("texas_hot", "999:999")

                assert result is None


class TestInsertHotel:
    """Tests for insert_hotel function."""

    @pytest.mark.asyncio
    async def test_skips_insert_when_external_id_exists(self):
        """Should return None when external_id already exists."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_external_id = AsyncMock(return_value={"id": 123})

                result = await repo.insert_hotel(
                    name="Test Hotel",
                    source="texas_hot",
                    external_id="123:456",
                    external_id_type="texas_hot",
                    city="Austin",
                    state="TX",
                )

                assert result is None
                mock_queries.insert_hotel_with_external_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_updates_existing_hotel_by_name_city(self):
        """Should update existing hotel when matched by name+city."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_external_id = AsyncMock(return_value=None)
                mock_queries.get_hotel_by_name_city = AsyncMock(return_value={"id": 456})
                mock_queries.update_hotel_from_ingestor = AsyncMock()
                mock_queries.update_hotel_external_id = AsyncMock()

                result = await repo.insert_hotel(
                    name="Test Hotel",
                    source="texas_hot",
                    external_id="123:456",
                    external_id_type="texas_hot",
                    city="Austin",
                    state="TX",
                    category="hotel",
                )

                assert result is None
                mock_queries.update_hotel_from_ingestor.assert_called_once()
                mock_queries.update_hotel_external_id.assert_called_once_with(
                    mock_conn,
                    hotel_id=456,
                    external_id="123:456",
                    external_id_type="texas_hot",
                )

    @pytest.mark.asyncio
    async def test_inserts_new_hotel_with_external_id(self):
        """Should insert new hotel when no duplicates found."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_external_id = AsyncMock(return_value=None)
                mock_queries.get_hotel_by_name_city = AsyncMock(return_value=None)
                mock_queries.insert_hotel_with_external_id = AsyncMock(return_value=789)

                result = await repo.insert_hotel(
                    name="New Hotel",
                    source="texas_hot",
                    external_id="123:456",
                    external_id_type="texas_hot",
                    city="Austin",
                    state="TX",
                )

                assert result == 789
                mock_queries.insert_hotel_with_external_id.assert_called_once()

    @pytest.mark.asyncio
    async def test_inserts_hotel_without_external_id(self):
        """Should insert hotel using legacy method when no external_id."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotel_by_name_city = AsyncMock(return_value=None)
                mock_queries.insert_hotel_with_category = AsyncMock(return_value=999)

                result = await repo.insert_hotel(
                    name="Manual Hotel",
                    source="manual",
                    city="Miami",
                    state="FL",
                )

                assert result == 999
                mock_queries.insert_hotel_with_category.assert_called_once()


class TestBatchInsertHotels:
    """Tests for batch_insert_hotels function."""

    @pytest.mark.asyncio
    async def test_filters_existing_external_ids(self):
        """Should filter out records with existing external_ids."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                # Simulate that "123:001" already exists
                mock_queries.get_hotels_by_external_ids = AsyncMock(
                    return_value=[{"external_id": "123:001"}]
                )

                records = [
                    ("Hotel A", "texas_hot", 0, "addr", "Austin", "TX", "USA", "555", "hotel", "123:001"),
                    ("Hotel B", "texas_hot", 0, "addr", "Dallas", "TX", "USA", "555", "hotel", "123:002"),
                ]

                result = await repo.batch_insert_hotels(records, external_id_type="texas_hot")

                # Should only insert 1 record (Hotel B)
                assert result == 1
                mock_conn.executemany.assert_called_once()
                call_args = mock_conn.executemany.call_args
                inserted_records = call_args[0][1]
                assert len(inserted_records) == 1
                assert inserted_records[0][0] == "Hotel B"

    @pytest.mark.asyncio
    async def test_returns_zero_when_all_exist(self):
        """Should return 0 when all external_ids already exist."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn
            with patch.object(repo, "queries") as mock_queries:
                mock_queries.get_hotels_by_external_ids = AsyncMock(
                    return_value=[
                        {"external_id": "123:001"},
                        {"external_id": "123:002"},
                    ]
                )

                records = [
                    ("Hotel A", "texas_hot", 0, "addr", "Austin", "TX", "USA", "555", "hotel", "123:001"),
                    ("Hotel B", "texas_hot", 0, "addr", "Dallas", "TX", "USA", "555", "hotel", "123:002"),
                ]

                result = await repo.batch_insert_hotels(records, external_id_type="texas_hot")

                assert result == 0
                mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_legacy_format_without_external_id(self):
        """Should handle legacy 9-element tuples without external_id."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn

            records = [
                ("Hotel A", "manual", 0, "addr", "Miami", "FL", "USA", "555", "hotel"),
                ("Hotel B", "manual", 0, "addr", "Orlando", "FL", "USA", "555", "hotel"),
            ]

            result = await repo.batch_insert_hotels(records)

            assert result == 2
            mock_conn.executemany.assert_called_once()
            call_args = mock_conn.executemany.call_args
            inserted_records = call_args[0][1]
            # Should have None, None appended for external_id, external_id_type
            assert len(inserted_records[0]) == 11
            assert inserted_records[0][9] is None
            assert inserted_records[0][10] is None


class TestBatchInsertRoomCounts:
    """Tests for batch_insert_room_counts function."""

    @pytest.mark.asyncio
    async def test_inserts_room_counts_by_external_id(self):
        """Should insert room counts with external_id lookup."""
        from services.ingestor import repo

        mock_conn = AsyncMock()

        with patch.object(repo, "get_conn") as mock_get_conn:
            mock_get_conn.return_value.__aenter__.return_value = mock_conn

            records = [
                (100, "123:001", "texas_hot"),
                (50, "123:002", "texas_hot"),
            ]

            result = await repo.batch_insert_room_counts(records, external_id_type="texas_hot")

            assert result == 2
            mock_conn.executemany.assert_called_once()
            call_args = mock_conn.executemany.call_args
            batch_records = call_args[0][1]
            # Format should be (room_count, external_id_type, external_id, source_name, confidence)
            assert batch_records[0] == (100, "texas_hot", "123:001", "texas_hot", 1.0)
            assert batch_records[1] == (50, "texas_hot", "123:002", "texas_hot", 1.0)

    @pytest.mark.asyncio
    async def test_returns_zero_without_external_id_type(self):
        """Should return 0 when external_id_type not provided."""
        from services.ingestor import repo

        records = [(100, "123:001", "texas_hot")]

        result = await repo.batch_insert_room_counts(records, external_id_type=None)

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_with_empty_records(self):
        """Should return 0 when records list is empty."""
        from services.ingestor import repo

        result = await repo.batch_insert_room_counts([], external_id_type="texas_hot")

        assert result == 0
