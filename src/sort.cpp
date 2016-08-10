//
//  sort.cpp
//  realm-object-store
//
//  Created by Realm on 8/10/16.
//
//

#include "sort.hpp"

#include <realm/views.hpp>

using namespace realm;

SortDescriptor TransientSortDescriptor::for_table(Table const& table) {
    return { table, m_column_indices, m_ascending };
}
